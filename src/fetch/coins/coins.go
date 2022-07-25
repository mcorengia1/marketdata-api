package coins

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	cgClient "api.jacarandapp.com/src/coingecko/client"
	cgTypes "api.jacarandapp.com/src/coingecko/types"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// Testing value
//const requestByMinute = 40

const requestByMinute = 10

var mongo_database = os.Getenv("MONGO_DATABASE")
var logger log.Logger = log.NewLogfmtLogger(os.Stdout)

const waitOnErrors = 60

/* La data desde la API de coingecko devuelve un valor del tipo *types.CoinsMarket
la informacion dentro es un json al que se le aplico unmarshal()
Peso aprox de todos los datos 28MB */
func UpdateCoinsInfo(cg *cgClient.Client, dbClient *mongo.Client) {

	defer errManagment()

	fetchStart := time.Now()
	coinsList, err := cg.CoinsList(false)

	if err != nil {
		level.Error(logger).Log("msg", "Coinslist request failed on coinsInfo update function", "ts", log.DefaultTimestampUTC(), "err", err)
	}

	lenCoinsList := len(*coinsList)

	/* Connect to the database */
	coll := dbClient.Database(mongo_database).Collection("coins_info")

	//Obtengo una lista de lo que falta en la db
	var coinListToAdd []cgTypes.CoinsListItem
	for i := 0; i < lenCoinsList; i++ {

		if (*coinsList)[i].ID == "" {
			continue
		}

		result := coll.FindOne(context.Background(), bson.D{{"id", (*coinsList)[i].ID}})

		if result.Err() != nil {
			// elemento no encontrado
			coinListToAdd = append(coinListToAdd, (*coinsList)[i])
		}
	}
	level.Info(logger).Log("msg", "CoinInfo database check done", "elements to add", len(coinListToAdd), "ts", log.DefaultTimestampUTC())

	//Si no hay que agregar nada me dedico a actualizar los elementos y sino empiezo a agregar los que faltan
	if len(coinListToAdd) > 0 {
		//add elements
		for i := 0; i < len(coinListToAdd); i++ {
			coin, err := cg.CoinsID((coinListToAdd)[i].ID, true, true, true, true, true, true)

			if err != nil || coin.ID != (coinListToAdd)[i].ID {
				//Si hay un error o por alguna razon no se trajo el elemento correcto se repite
				level.Error(logger).Log("msg", "CoinInfo request failed, waiting to retry", "id", (*coinsList)[i].ID, "err", err, "ts", log.DefaultTimestampUTC())
				time.Sleep(time.Duration(time.Second * 60))
				level.Info(logger).Log("msg", "Continuing with coinInfo requests", "ts", log.DefaultTimestampUTC())
				i--
				continue
			} else {
				_, err = coll.InsertOne(context.Background(), cgClient.CoinInfoDBCreate(*coin))

				if err != nil {
					level.Error(logger).Log("msg", "Coininfo database insert new element failed", "ts", log.DefaultTimestampUTC(), "err", err)
				}
			}

			completed := i * 100 / len(coinListToAdd)
			switch completed {
			case 25:
				level.Info(logger).Log("msg", "CoinInfo updating 25%% complete", "ts", log.DefaultTimestampUTC())
			case 50:
				level.Info(logger).Log("msg", "CoinInfo updating 50%% complete", "ts", log.DefaultTimestampUTC())
			case 75:
				level.Info(logger).Log("msg", "CoinInfo updating 75%% complete", "ts", log.DefaultTimestampUTC())
			}
			time.Sleep(time.Duration(time.Second * 60 / requestByMinute))
		}
	} else {
		//update elements
		for i := 0; i < len(*coinsList); i++ {
			coin, errReq := cg.CoinsID((*coinsList)[i].ID, true, true, true, true, true, true)
			result, errDB := coll.ReplaceOne(context.Background(), bson.D{{"id", coin.ID}}, cgClient.CoinInfoDBCreate(*coin))

			if errReq != nil || coin.ID != (*coinsList)[i].ID {
				level.Error(logger).Log("msg", "Coininfo database update request failed", "ts", log.DefaultTimestampUTC(), "err", errReq)
				i--
				continue

			} else if errDB != nil {
				level.Error(logger).Log("msg", "Coininfo database update database failed", "ts", log.DefaultTimestampUTC(), "err", errDB)
				i--
				continue

			} else if result.ModifiedCount == 0 {
				level.Error(logger).Log("msg", "Coininfo database update not modified documents", "ts", log.DefaultTimestampUTC(), "err", err)
			}

			completed := i * 100 / len(*coinsList)
			switch completed {
			case 25:
				level.Info(logger).Log("msg", "CoinInfo updating 25%% complete", "ts", log.DefaultTimestampUTC())
			case 50:
				level.Info(logger).Log("msg", "CoinInfo updating 50%% complete", "ts", log.DefaultTimestampUTC())
			case 75:
				level.Info(logger).Log("msg", "CoinInfo updating 75%% complete", "ts", log.DefaultTimestampUTC())
			}
			time.Sleep(time.Duration(time.Second * 60 / requestByMinute))
		}
	}

	// logs
	lastUpdate := time.Now()
	fetchDuration := lastUpdate.Sub(fetchStart)
	dbSize, err := coll.CountDocuments(context.Background(), bson.D{})
	level.Info(logger).Log("msg", "CoinInfo database updated", "update_duration", fetchDuration, "database size", dbSize, "ts", log.DefaultTimestampUTC())
}

func GetCoinInfoById(mongoClient *mongo.Client, id string) cgTypes.CoinInfoDB {

	coll := mongoClient.Database(mongo_database).Collection("coins_info")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var data cgTypes.CoinInfoDB
	err := coll.FindOne(ctx, bson.D{{"id", id}}).Decode(&data)
	if err != nil {
		level.Error(logger).Log("msg", "No results in coinInfo db by id", "id", id, "ts", log.DefaultTimestampUTC(), "err", err)
	}

	return data
}

func GetCoinInfoByContract(mongoClient *mongo.Client, contract string) cgTypes.CoinInfoDB {

	coll := mongoClient.Database(mongo_database).Collection("coins_info")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var data cgTypes.CoinInfoDB
	err := coll.FindOne(ctx, bson.D{{"platforms_value", contract}}).Decode(&data)
	if err != nil {
		level.Error(logger).Log("msg", "No results in coinInfo db by contract", "contract", contract, "ts", log.DefaultTimestampUTC(), "err", err)
	}

	return data
}

func GetReductedCoinsInfo(mongoClient *mongo.Client) []cgTypes.ReductedCoinInfo {

	var data []cgTypes.ReductedCoinInfo
	var coinInfoDB cgTypes.CoinInfoDB

	coll := mongoClient.Database(mongo_database).Collection("coins_info")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer errManagment()

	//Obtengo el cursor para iterar la coleccion
	cursor, err := coll.Find(ctx, bson.D{})

	if err != nil {
		level.Error(logger).Log("msg", "Find function error in getReductedInfo", "ts", log.DefaultTimestampUTC(), "err", err)
	}

	for cursor.Next(ctx) {
		err := cursor.Decode(&coinInfoDB)

		if err != nil {
			level.Error(logger).Log("msg", "Cannot decode cursor data in getReductedInfo", "ts", log.DefaultTimestampUTC(), "err", err)
		}

		data = append(data, ReductedDataCreate(coinInfoDB))
	}

	return data
}

func ReductedDataCreate(coinInfo cgTypes.CoinInfoDB) cgTypes.ReductedCoinInfo {

	var data cgTypes.ReductedCoinInfo
	var temp int

	data.Name = coinInfo.Name
	data.Symbol = coinInfo.Symbol
	data.Id = coinInfo.ID

	temp = len(coinInfo.PlatformsKey)
	data.Platforms = make(map[string]string)

	for i := 0; i < temp; i++ {
		data.Platforms[coinInfo.PlatformsKey[i]] = coinInfo.PlatformsValue[i]
	}

	data.MarketCapRank = coinInfo.MarketCapRank
	data.CoinGeckoRank = coinInfo.CoinGeckoRank
	data.CoinGeckoScore = coinInfo.CoinGeckoScore
	data.DeveloperScore = coinInfo.DeveloperScore
	data.CommunityScore = coinInfo.CommunityScore
	data.LiquidityScore = coinInfo.LiquidityScore
	data.PublicInterestScore = coinInfo.PublicInterestScore

	data.Categories = coinInfo.Categories
	data.ImageThumbUrl = coinInfo.ImageThumb
	data.ImageSmallUrl = coinInfo.ImageSmall
	data.ImageLargeUrl = coinInfo.ImageLarge

	return data
}

func errManagment() {
	recoverInfo := recover()
	if recoverInfo != nil {
		fmt.Println(recoverInfo)

		fmt.Println("Waiting 60sec before continue with the requests")
		time.Sleep(time.Duration(time.Second * waitOnErrors))
	}
}
