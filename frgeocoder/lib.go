package frgeocoder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	//"github.com/codingsince1985/geo-golang/geocod"
	// "github.com/codingsince1985/geo-golang/locationiq"

	"github.com/codingsince1985/geo-golang"
	"github.com/codingsince1985/geo-golang/chained"
	"github.com/codingsince1985/geo-golang/mapbox"
	"github.com/codingsince1985/geo-golang/openstreetmap"
	"github.com/go-spatial/geom"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	geojson "github.com/paulmach/go.geojson"
)

////////////////////////////////////////////////////////////////////////////
//
var (
	dbMutex        sync.Mutex
	GEO_BOUNDARIES = geom.Extent{30.406366, -97.923777, 30.656545, -97.343905}
	GEOCODER       *geo.Geocoder
	DB             *pgxpool.Pool
	S3CLNT         *s3.Client
)

////////////////////////////////////////////////////////////////////////////
//
func Init() error {
	if DB == nil {
		dbMutex.Lock()
		defer dbMutex.Unlock()

		if DB == nil {

			s3client, err := getS3Client()
			if err != nil {
				return err
			}
			S3CLNT = s3client

			cnxn, err := makeDbConnection()
			if err != nil {
				return err
			}
			DB = cnxn
			if GEOCODER == nil {
				GEOCODER = getGeocoder()
			}
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////
//
func Deinit() {
	if DB != nil {
		DB.Close()
	}
}

func getS3Client() (*s3.Client, error) {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return nil, err
	}

	s3Client := s3.NewFromConfig(sdkConfig)
	return s3Client, nil
}

////////////////////////////////////////////////////////////////////////////
//
func makeDbConnection() (*pgxpool.Pool, error) {

	dbId := os.Getenv("DB_ID")
	dbToken := os.Getenv("DB_TOKEN")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbCaRoot := os.Getenv("DB_CA_ROOT_PATH")

	dbName := "defaultdb"
	cluster := "pushy-iguana-1562"

	dbOptions := url.PathEscape(fmt.Sprintf("--cluster=%s", cluster))
	dbParams := fmt.Sprintf("%s?sslmode=verify-full&sslrootcert=%s&options=%s", dbName, dbCaRoot, dbOptions)
	cnxnUri := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", dbId, dbToken, dbHost, dbPort, dbParams)
	// Attempt to connect
	// log.Println("\n\nCnxn String: ", cnxnUri, "\n")
	conn, err := pgxpool.New(context.Background(), cnxnUri)
	if err != nil {
		log.Printf("Failed new db connection: %v", err)
		return nil, err
	}
	// defer conn.Close()
	return conn, nil
}

////////////////////////////////////////////////////////////////////////////
//
func getGeocoder() *geo.Geocoder {
	geocoder := chained.Geocoder(
		openstreetmap.Geocoder(),
		mapbox.Geocoder(os.Getenv("MAPBOX_API_KEY")),
	)

	// Other Geocoders to look at if we need to...
	//"github.com/codingsince1985/geo-golang/geocod"
	// "github.com/codingsince1985/geo-golang/locationiq"
	// geocod.Geocoder(os.Getenv("GEOCOD_API_KEY")),
	// locationiq.Geocoder(os.Getenv("LOCATIONIQ_API_KEY"), zoom)
	return &geocoder
}

////////////////////////////////////////////////////////////////////////////
//
func tryGeocoding(addr *string) (string, string) {
	location, _ := (*GEOCODER).Geocode(*addr)
	if location != nil {
		//log.Printf("%s location is (%.7f, %.7f)\n", *addr, location.Lat, location.Lng)
		if GEO_BOUNDARIES.ContainsPoint(geom.Point{location.Lat, location.Lng}) {
			return fmt.Sprintf("%.7f", location.Lat), fmt.Sprintf("%.7f", location.Lng)
		} else {
			return "INV", "INV"
		}
	} else {
		// log.Println("got <nil> location")
		return "NF", "NF"
	}
}

////////////////////////////////////////////////////////////////////////////
//
type Location struct {
	Id           string
	Addr         string
	Zipcode      int
	Neighborhood string
	City         string
	Lat          string
	Lng          string
}

////////////////////////////////////////////////////////////////////////////
//
func getOrdersWithoutKnownLocations(ctx context.Context) ([]Location, error) {
	log.Println("Getting orders with unknown locations")

	sqlCmd := "SELECT order_id, customer_addr1, customer_addr2, customer_city, customer_zipcode, customer_neighborhood" +
		" FROM mulch_orders WHERE known_addr_id IS NULL AND customer_neighborhood NOT LIKE 'Out of Area%'"

	rows, err := DB.Query(context.Background(), sqlCmd)
	if err != nil {
		log.Println("Orders with unknown location query failed", err)
		return nil, err
	}
	defer rows.Close()

	results := []Location{}

	for rows.Next() {
		rslt := Location{}

		var addr2 *string
		err = rows.Scan(&rslt.Id, &rslt.Addr, &addr2, &rslt.City, &rslt.Zipcode, &rslt.Neighborhood)
		if err != nil {
			log.Println("Reading row failed: ", err)
			return nil, err
		}

		rslt.City = strings.TrimSpace(rslt.City)
		rslt.Addr = strings.TrimSpace(rslt.Addr)
		if addr2 != nil {
			*addr2 = strings.TrimSpace(*addr2)
			rslt.Addr = fmt.Sprintf("%s %s", rslt.Addr, *addr2)
		}

		results = append(results, rslt)
	}

	if rows.Err() != nil {
		log.Println("Reading order rows had an issue: ", err)
		return nil, err
	}
	return results, nil
}

////////////////////////////////////////////////////////////////////////////
//
func getKnownLocations(ctx context.Context) ([]Location, error) {
	log.Println("Getting known locations")

	sqlCmd := "SELECT id, addr, city, zipcode, lat, lng FROM known_addrs"

	rows, err := DB.Query(context.Background(), sqlCmd)
	if err != nil {
		log.Println("Known location query failed", err)
		return nil, err
	}
	defer rows.Close()

	results := []Location{}

	for rows.Next() {
		rslt := Location{}

		err = rows.Scan(&rslt.Id, &rslt.Addr, &rslt.City, &rslt.Zipcode, &rslt.Lat, &rslt.Lng)
		if err != nil {
			log.Println("Reading known locations row failed: ", err)
			return nil, err
		}

		// Lower case them now so we don't have to keep doing it
		rslt.City = strings.ToLower(rslt.City)
		rslt.Addr = strings.ToLower(rslt.Addr)

		results = append(results, rslt)
	}

	if rows.Err() != nil {
		log.Println("Reading known locations rows had an issue: ", err)
		return nil, err
	}
	return results, nil
}

////////////////////////////////////////////////////////////////////////////
//
func addNewLoc(ctx context.Context, loc Location, trxn *pgx.Tx) (string, error) {
	newId := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	log.Printf("Adding New Location: %s, %s, %d, %s, %s, %s", newId, loc.Addr, loc.Zipcode, loc.City, loc.Lat, loc.Lng)
	sqlCmd := "INSERT INTO known_addrs(id, addr, zipcode, city, lat, lng, last_modified_time, created_time)" +
		" values ($1::uuid, $2, $3, $4, $5, $6, $7::timestamp, $7::timestamp)"

	_, err := (*trxn).Exec(context.Background(), sqlCmd, newId, loc.Addr, loc.Zipcode, loc.City, loc.Lat, loc.Lng, now)
	if err != nil {
		log.Printf("Failed Inserting: %v", err)
		return "", err
	}

	return newId, nil
}

////////////////////////////////////////////////////////////////////////////
//
func updateOrder(ctx context.Context, locId string, orderId string, trxn *pgx.Tx) error {
	log.Printf("Updating mulch order associating locid to orderid %s -> %s", locId, orderId)
	sqlCmd := "UPDATE mulch_orders SET known_addr_id=$1::uuid WHERE order_id=$2"
	_, err := (*trxn).Exec(context.Background(), sqlCmd, locId, orderId)
	if err != nil {
		log.Printf("Failed Updating Mulch Order: %v", err)
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////
//
func convertLatLngStrToGeoJsonPt(lat_str string, lng_str string) ([]float64, error) {

	lat, err := strconv.ParseFloat(lat_str, 64)
	if err != nil {
		return []float64{}, err
	}
	lng, err := strconv.ParseFloat(lng_str, 64)
	if err != nil {
		return []float64{}, err
	}
	// GeoJSON wants lng, lat
	return []float64{lng, lat}, nil
}

////////////////////////////////////////////////////////////////////////////
//
func updateDbLocations(ctx context.Context) error {

	collection, err := getGeoJson(ctx)
	if err != nil {
		log.Printf("Failed getting GeoJSON: %v", err)
		return err
	}

	log.Printf("GeoJSON Loaded: %v", *collection)
	// Get Unkown orders
	unknownLocs, err := getOrdersWithoutKnownLocations(ctx)
	if err != nil {
		//log.Println("Filed getting orders with unknown location query failed", err)
		return err
	}
	if len(unknownLocs) == 0 {
		log.Println("No orders found with unknown locations")
		return nil
	}

	// Get Known Orders
	knownLocs, err := getKnownLocations(ctx)
	if err != nil {
		//log.Println("Filed getting orders with unknown location query failed", err)
		return err
	}

	trxn, err := DB.Begin(context.Background())
	if err != nil {
		return err
	}
	//defer trxn.Rollback(context.Background())
	// See if known_locs has unkown_loc
	for _, unknownLoc := range unknownLocs {
		resolvedLocUuid := ""
		for _, knownLoc := range knownLocs {
			if strings.ToLower(unknownLoc.Addr) == knownLoc.Addr &&
				unknownLoc.Zipcode == knownLoc.Zipcode &&
				strings.ToLower(unknownLoc.City) == knownLoc.City {
				log.Println("Found Match associating known location: ", knownLoc)
				resolvedLocUuid = knownLoc.Id
				unknownLoc.Lat = knownLoc.Lat
				unknownLoc.Lng = knownLoc.Lng
				break
			}

		}

		if len(resolvedLocUuid) == 0 {
			log.Println("A match was not found with a known location")
			// Match not found so resolve it

			queryAddr := fmt.Sprintf("%s, %s, Texas, %d", unknownLoc.Addr, unknownLoc.City, unknownLoc.Zipcode)
			lat, lng := tryGeocoding(&queryAddr)
			log.Printf("Adding Resolved Address:\n%s\nTo: (%s, %s)", queryAddr, lat, lng)

			unknownLoc.Lat = lat
			unknownLoc.Lng = lng
			// Add it to known table
			resolvedLocUuid, err = addNewLoc(ctx, unknownLoc, &trxn)
			if err != nil {
				return err
			}
		}

		if len(unknownLoc.Lat) != 0 && unknownLoc.Lat != "NF" && unknownLoc.Lat != "INV" {
			coords, err := convertLatLngStrToGeoJsonPt(unknownLoc.Lat, unknownLoc.Lng)
			if err != nil {
				log.Println("Failed to convert lat/lng string to float: ", unknownLoc)
			} else {

				collection.AddFeature(geojson.NewPointFeature(coords))
			}
		}

		// Associate order with resolved UUID
		updateOrder(ctx, resolvedLocUuid, unknownLoc.Id, &trxn)
	}

	err = saveGeoJson(ctx, collection)
	if err != nil {
		log.Printf("Failed to save GeoJSON: %v", err)
		return err
	}

	log.Println("About to make a commitment")
	err = trxn.Commit(context.Background())
	if err != nil {
		log.Printf("Failed to make a commitment: %v", err)
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////
//
func getGeoJson(ctx context.Context) (*geojson.FeatureCollection, error) {
	//geoJsonUri := "s3://t27fundraiser/T27FundraiserGeoJSON.json"
	bucketName := os.Getenv("T27FR_S3_BUCKETNAME")
	objectKey := os.Getenv("T27FR_S3_GEOJSON")

	result, err := S3CLNT.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't get object %v:%v. Here's why: %v\n", bucketName, objectKey, err)
		return nil, err
	}
	defer result.Body.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Couldn't read object body from %v. Here's why: %v\n", objectKey, err)
		return nil, err
	}

	featureCollection, err := geojson.UnmarshalFeatureCollection(body)
	if err != nil {
		log.Printf("Couldn't unmarshal feature collection. Here's why: %v\n", err)
		return nil, err
	}
	return featureCollection, nil

}

////////////////////////////////////////////////////////////////////////////
//
func saveGeoJson(ctx context.Context, collection *geojson.FeatureCollection) error {
	//geoJsonUri := "s3://t27fundraiser/T27FundraiserGeoJSON.json"
	bucketName := os.Getenv("T27FR_S3_BUCKETNAME")
	objectKey := os.Getenv("T27FR_S3_GEOJSON")

	log.Printf("Saving Collection: %v", *collection)
	rawJSON, err := collection.MarshalJSON()
	if err != nil {
		log.Printf("Couldn't marshal feature collection. Here's why: %v", err)
		return err
	}

	_, err = S3CLNT.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(rawJSON),
	})
	if err != nil {
		log.Printf("Couldn't upload to %v:%v. Here's why: %v", bucketName, objectKey, err)
		return err
	}

	return nil

}

////////////////////////////////////////////////////////////////////////////
//
func regenGeoJSON(ctx context.Context) error {
	log.Println("Getting known locations")

	sqlCmd := "SELECT lat,lng FROM mulch_orders JOIN known_addrs ON known_addr_id=id"

	rows, err := DB.Query(context.Background(), sqlCmd)
	if err != nil {
		log.Println("Known location query failed", err)
		return err
	}
	defer rows.Close()

	collection := geojson.NewFeatureCollection()

	for rows.Next() {
		lat_str := ""
		lng_str := ""

		err = rows.Scan(&lat_str, &lng_str)
		if err != nil {
			log.Println("Reading lat,lng row failed: ", err)
			return err
		}

		if len(lat_str) != 0 && lat_str != "NF" && lat_str != "INV" {
			coords, err := convertLatLngStrToGeoJsonPt(lat_str, lng_str)
			if err != nil {
				log.Printf("Failed to convert lat/lng string to float: %s,%s", lat_str, lng_str)
			} else {
				collection.AddFeature(geojson.NewPointFeature(coords))
			}
		}

	}

	if rows.Err() != nil {
		log.Println("Reading known locations rows had an issue: ", err)
		return err
	}

	err = saveGeoJson(ctx, collection)
	if err != nil {
		log.Println("Failed saving collection: ", err)
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////
//
type UpdateGeoJsonFlags struct {
	DoCompleteRegen bool
	UpdateDb        bool
}

////////////////////////////////////////////////////////////////////////////
//
func UpdateGeoJson(ctx context.Context, flags UpdateGeoJsonFlags) error {

	if flags.UpdateDb {
		err := updateDbLocations(ctx)
		if err != nil {
			log.Printf("Failed to Update DB Locations: %v", err)
			return err
		}

	} else if flags.DoCompleteRegen {
		err := regenGeoJSON(ctx)
		if err != nil {
			log.Printf("Failed to Regen GeoJSON: %v", err)
			return err
		}
	}

	return nil
}
