package Historical

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	cgClient "api.jacarandapp.com/src/coingecko/client"
	cgTypes "api.jacarandapp.com/src/coingecko/types"
	utils "api.jacarandapp.com/src/utils"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

//const requestByMinute = 10

//testing value
const requestByMinute = 15

//var mongo_database = os.Getenv("MONGO_DATABASE")
var mongo_database = "historical"
var logger log.Logger = log.NewLogfmtLogger(os.Stdout)

const waitOnErrors = 60

// type Entry struct {
// 	Timestamp int64   `json:"timestamp" bson:"timestamp"`
// 	Volume    float64 `json:"volume" bson:"volume"`
// 	Price     float64 `json:"price" bson:"price"`
// 	Marketcap float64 `json:"marketcap" bson:"marketcap"`
// }

type Entry struct {
	Timestamp primitive.DateTime `json:"timestamp" bson:"timestamp"`
	Volume    float64            `json:"volume" bson:"volume"`
	Price     float64            `json:"price" bson:"price"`
	Marketcap float64            `json:"marketcap" bson:"marketcap"`
}

type NoUpdate struct {
	ID string `json:"id" bson:"id"`
}

/*
todos los dias a las 01:00
pido la coinlist y veo si esta el historico de todas, si no existe agrego esa collection
cada collection si tiene mas de un elemento, el ultimo elemento del array no lo inserto (el mas actual)
una vez que se hizo esa comprobacion inserto en todas las collections la data del dia sacada de marketdata

MEJOR NADA MAS ELIMINO LA ULTIMA ENTRADA OSEA EL QUE NO ESTA A LAS 00 Y DEJO TODO IGUAL GOOD ENOGHT
a las 00:00 se agregan los valores en coingecko
a las 01:00 actualizo los mktdata de todas las collections, reviso si la ultima entrada tiene una diferencia de aprox 24/25hs
*/

func UpdateHistoricalDB(cg *cgClient.Client, dbClient *mongo.Client) {
	defer errManagment()

	coinsList, err := cg.CoinsList(false)
	if err != nil {
		level.Error(logger).Log("msg", "Coinslist request failed on historical info update function", "ts", log.DefaultTimestampUTC(), "err", err)
	}

	/* Connect to the database */
	db := dbClient.Database(mongo_database)
	/* Get all the collections */
	collections, err := getAllCollectionsNames(db)

	if err != nil {
		level.Error(logger).Log("msg", "Error getting the collections names", "ts", log.DefaultTimestampUTC(), "err", err)
	}

	for i := 0; i < len(*coinsList); i++ {

		found := false
		for j := 0; j < len(collections); j++ {

			if (*coinsList)[i].ID == collections[j] {

				// The historical data of that crypto is already loaded
				found = true
				break
			}
		}

		if !found {
			// No esta el historico de esa crypto asi que tengo que traer todo entero
			err = setHistoricalData((*coinsList)[i].ID, db, cg)
			if err != nil {
				level.Error(logger).Log("msg", "Error getting the market chart", "ts", log.DefaultTimestampUTC(), "err", err)
			}

			fmt.Println("No hay historico, traigo todo" + (*coinsList)[i].ID)
		}
	}
}

func getAllCollectionsNames(db *mongo.Database) ([]string, error) {
	//return db.ListCollectionNames(context.Background(), bson.D{}, options.ListCollections().SetNameOnly(true))
	collections, err := db.ListCollectionNames(context.Background(), bson.D{}, options.ListCollections().SetNameOnly(true))

	//ningun ID tiene un '.' asi que filtro por eso para eliminar los valores del sistema
	var names []string
	for i := 0; i < len(collections); i++ {

		if !strings.Contains(collections[i], ".") {
			names = append(names, collections[i])
		}
	}

	return names, err
}

//Agrega el ultimo elemento en cada collection si corresponde
func UpdateHistoricalMktData(cg *cgClient.Client, dbClient *mongo.Client, marketData *[]cgTypes.CoinMarketData) {
	defer errManagment()

	/* Connect to the database */
	db := dbClient.Database(mongo_database)
	/* Get all the collections */
	collections, err := getAllCollectionsNames(db)

	if err != nil {
		level.Error(logger).Log("msg", "Error getting the collections names", "ts", log.DefaultTimestampUTC(), "err", err)
	}

	for i := 0; i < len(collections); i++ {

		if haveToUpdate(db, collections[i]) {
			//Si es algo actualizable

			coll := db.Collection(collections[i])
			// Get the last entry
			opts := options.FindOne().SetSort(bson.M{"_id": -1})
			result := coll.FindOne(context.Background(), bson.D{}, opts)

			//Encontro un ultimo elemento
			if result.Err() == nil {

				var lastEntry Entry
				err := result.Decode(&lastEntry)
				if err != nil {
					level.Error(logger).Log("msg", "Cannot decode last entry", "ts", log.DefaultTimestampUTC(), "err", err)
				}

				// Actualizo el mktdata si la diferencia es 24hs +-1hour
				if time.Now().Unix()-lastEntry.Timestamp.Time().Unix() > 82800 && time.Now().Unix()-lastEntry.Timestamp.Time().Unix() < 90000 {

					currentMkt := utils.GetElementsById(marketData, &[]string{collections[i]})

					//Agrego los datos actuales del marketdata
					newEntry := Entry{Timestamp: primitive.NewDateTimeFromTime(time.Now().UTC()), Price: currentMkt[0].MarketData.CurrentPrice,
						Marketcap: currentMkt[0].MarketData.MarketCap, Volume: currentMkt[0].MarketData.TotalVolume}

					// Insert in the collection
					_, err := coll.InsertOne(context.Background(), newEntry)

					if err != nil {
						level.Error(logger).Log("msg", "Cannot insert the new marketdata", "ts", log.DefaultTimestampUTC(), "err", err)
					}

				} else if time.Now().Unix()-lastEntry.Timestamp.Time().Unix() > 90000 {

					// Es un elemento actualizable pero por alguna razon me faltan datos
					// Drop collection and get it again
					err = db.Collection(collections[i]).Drop(context.Background())
					if err != nil {
						level.Error(logger).Log("msg", "Cannot drop the given collection", "ts", log.DefaultTimestampUTC(), "coll", collections[i], "err", err)
					}
					err = setHistoricalData(collections[i], db, cg)
					if err != nil {
						level.Error(logger).Log("msg", "Error getting the market chart", "ts", log.DefaultTimestampUTC(), "err", err)
					}
				}
			}
		}
	}
}

func setHistoricalData(ID string, db *mongo.Database, cg *cgClient.Client) error {

	defer errManagment()
	time.Sleep(time.Duration(time.Second * 60 / requestByMinute))

	tso := options.TimeSeries().SetTimeField("timestamp").SetGranularity("hours")
	opts := options.CreateCollection().SetTimeSeriesOptions(tso)
	db.CreateCollection(context.Background(), ID, opts)

	history, err := cg.CoinsIDMarketChart(ID, "usd", "max")

	if err != nil {
		return err
	}

	//No pongo las 2 condiciones juntas porque sino puede dar error
	if len(*history.Prices) <= 1 {
		//Si no hay valores o uno solo no se actualiza
		aux := NoUpdate{ID: ID}
		_, err := db.Collection("no-update").InsertOne(context.Background(), aux)
		if err != nil {
			return err

		} else {
			return nil
		}

		//Divido por 1000 para pasar de milisegundos a segundos en unix
		//Si el ultimo valor es mayor a 2 dias entonces lo agrego a 'no actualizables'
	} else if time.Now().Unix()-int64((*history.Prices)[len(*history.Prices)-1][0]/1000) > 169200 {

		aux := NoUpdate{ID: ID}
		_, err := db.Collection("no-update").InsertOne(context.Background(), aux)
		if err != nil {
			return err

		} else {
			return nil
		}
	}

	//No traigo el ultimo elemento ya que esa es la cotizacion actual
	entries := make([]interface{}, len((*history.Prices))-1)

	for i := 0; i < len(entries); i++ {

		entries[i] = Entry{Timestamp: primitive.NewDateTimeFromTime(time.Unix((int64((*history.Prices)[i][0]))/1000, 0).UTC()),
			Price: float64((*history.Prices)[i][1]), Volume: float64((*history.TotalVolumes)[i][1]),
			Marketcap: float64((*history.MarketCaps)[i][1])}
	}

	//Se insertan en orden cronologico ascendente?
	_, err = db.Collection(ID).InsertMany(context.Background(), entries)
	if err != nil {
		level.Error(logger).Log("msg", "Cannot insert the entry in the historical database", "ts", log.DefaultTimestampUTC(), "err", err)
		return err

	} else {

		return nil
	}
}

func haveToUpdate(db *mongo.Database, ID string) bool {
	result := db.Collection("no-update").FindOne(context.Background(), bson.D{{"id", ID}})

	return result.Err() != nil
}

func errManagment() {
	recoverInfo := recover()
	if recoverInfo != nil {
		fmt.Println(recoverInfo)
		fmt.Println("RECOVERING FROM HISTORICAL INFO")

		fmt.Println("Waiting 60sec before continue with the requests")
		time.Sleep(time.Duration(time.Second * waitOnErrors))
	}
}

// func UpdateHistoricalDB(cg *cgClient.Client, dbClient *mongo.Client, marketData *[]cgTypes.CoinMarketData) {
// 	/*
// 	   a la 1am reviso toda la base de datos(esto en el main)
// 	   si el ultimo valor agregado es de ayer
// 	   agrego una nueva entrada con los datos de mktdata
// 	   sino es significa que se me pasaron mas datos
// 	   droppeo toda la colleccion y la pido entera de vuelta
// 	*/
// 	defer errManagment()

// 	coinsList, err := cg.CoinsList(false)
// 	if err != nil {
// 		level.Error(logger).Log("msg", "Coinslist request failed on historical info update function", "ts", log.DefaultTimestampUTC(), "err", err)
// 	}

// 	/* Connect to the database */
// 	db := dbClient.Database(mongo_database)
// 	/* Get all the collections */
// 	collections, err := db.ListCollectionNames(context.Background(), bson.D{}, options.ListCollections().SetNameOnly(true))

// 	if err != nil {
// 		level.Error(logger).Log("msg", "Error getting the collections names", "ts", log.DefaultTimestampUTC(), "err", err)
// 	}

// 	for i := 0; i < len(*coinsList); i++ {

// 		if haveToUpdate(db, (*coinsList)[i].ID) {

// 			needToWait := true
// 			found := false
// 			for j := 0; j < len(collections); j++ {

// 				if (*coinsList)[i].ID == collections[j] {

// 					// The historical data of that crypto is already loaded
// 					found = true
// 					coll := db.Collection(collections[j])

// 					// Get the last entry
// 					opts := options.FindOne().SetSort(bson.M{"_id": -1})
// 					result := coll.FindOne(context.Background(), bson.D{}, opts)

// 					var lastEntry Entry
// 					err := result.Decode(&lastEntry)
// 					if err != nil {
// 						level.Error(logger).Log("msg", "Cannot decode last entry", "ts", log.DefaultTimestampUTC(), "err", err)
// 					}

// 					// Diferencia mayor a dos dias traigo todo de vuelta +47hs
// 					if time.Now().Unix()-lastEntry.Timestamp.Time().Unix() > 169200 {
// 						// Drop collection and get it again
// 						err = db.Collection(collections[j]).Drop(context.Background())
// 						if err != nil {
// 							level.Error(logger).Log("msg", "Cannot drop the given collection", "ts", log.DefaultTimestampUTC(), "coll", collections[j], "err", err)
// 						}
// 						err = setHistoricalData(collections[j], db, cg)
// 						if err != nil {
// 							level.Error(logger).Log("msg", "Error getting the market chart", "ts", log.DefaultTimestampUTC(), "err", err)
// 						}

// 						fmt.Println("Diferencia > 2d todo de vuelta")
// 						fmt.Println(lastEntry.Timestamp.Time())
// 						fmt.Println("*******************")

// 						// Diferencia menor de dos dias pero el ultimo valor no es de hoy +23hs
// 					} else if time.Now().Unix()-lastEntry.Timestamp.Time().Unix() > 82800 {
// 						needToWait = false
// 						//var currentMkt = make([]cgTypes.CoinMarketData, 1)
// 						currentMkt := utils.GetElementsById(marketData, &[]string{collections[j]})

// 						if len(currentMkt) == 0 {
// 							level.Error(logger).Log("msg", "No marketdata available for id", "id", collections[j], "ts", log.DefaultTimestampUTC(), "err", err)
// 							continue
// 						} else {
// 							//Agrego los datos actuales del marketdata
// 							newEntry := Entry{Timestamp: primitive.NewDateTimeFromTime(time.Now().UTC()), Price: currentMkt[0].MarketData.CurrentPrice,
// 								Marketcap: currentMkt[0].MarketData.MarketCap, Volume: currentMkt[0].MarketData.TotalVolume}

// 							// Insert in the collection
// 							coll.InsertOne(context.Background(), newEntry)
// 							fmt.Println("Diferencia < 2d tengo que actualizar con mkt data")
// 						}

// 					} else {
// 						//Si ya guarde un elemento por hoy no hago nada
// 						needToWait = false
// 					}
// 				}
// 			}

// 			if !found {
// 				// No esta el historico de esa crypto asi que tengo que traer todo entero
// 				err = setHistoricalData((*coinsList)[i].ID, db, cg)
// 				if err != nil {
// 					level.Error(logger).Log("msg", "Error getting the market chart", "ts", log.DefaultTimestampUTC(), "err", err)
// 				}

// 				fmt.Println("No hay historico, traigo todo" + (*coinsList)[i].ID)
// 			}

// 			if needToWait {
// 				time.Sleep(time.Duration(time.Second * 60 / requestByMinute))
// 			}
// 		}
// 	}
// }
