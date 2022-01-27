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
)

const requestByMinute = 10

/* La data desde la API de coingecko devuelve un valor del tipo *types.CoinsMarket
la informacion dentro es un json al que se le aplico unmarshal()
Peso aprox de todos los datos 28MB */
func UpdateCoinsInfo(cg *cgClient.Client, dbClient *mongo.Client) {

	MONGO_DATABASE := os.Getenv("MONGO_DATABASE")

	fetchStart := time.Now()
	coinsList, _ := cg.CoinsList(false)
	lenCoinsList := len(*coinsList)

	/* Connect to the database */
	coll := dbClient.Database(MONGO_DATABASE).Collection("coins_info")

	for i := 0; i < lenCoinsList; i++ {

		coin, err := cg.CoinsID((*coinsList)[i].ID, true, true, true, true, true, true)

		if err != nil || coin.ID != (*coinsList)[i].ID {
			//Si hay un error o por alguna razon no se trajo el elemento correcto se repite
			fmt.Println(err)
			fmt.Println("Server error, waiting 60sec to continue with the requests")
			time.Sleep(time.Duration(time.Second * 60))
			i--
			continue
		} else {

			result, _ := coll.ReplaceOne(context.Background(), bson.D{{"id", coin.ID}}, cgClient.CoinInfoDBCreate(*coin))

			if result.ModifiedCount == 0 {
				coll.InsertOne(context.Background(), cgClient.CoinInfoDBCreate(*coin))
			}

		}

		fmt.Println("Working on ", i, "/", lenCoinsList)
		//Tiempo de espera segun el limite de peticiones al servidor de coingecko
		time.Sleep(time.Duration(time.Second * 60 / requestByMinute))
	}

	lastUpdate := time.Now()
	fetchDuration := lastUpdate.Sub(fetchStart)
	fmt.Println(fetchDuration)
}

func GetCoinInfoById(mongoClient *mongo.Client, id string) cgTypes.CoinInfoDB {

	coll := mongoClient.Database("testing").Collection("coins_info")
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	var data cgTypes.CoinInfoDB
	err := coll.FindOne(ctx, bson.D{{"id", id}}).Decode(&data)
	if err != nil {
		fmt.Println(err)
	}

	return data
}
