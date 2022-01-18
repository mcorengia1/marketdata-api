package fetch

import (
	"fmt"
	"log"
	"time"

	cgClient "api.jacarandapp.com/src/coingecko/client"
	cgTypes "api.jacarandapp.com/src/coingecko/types"
)

const requestByMinute = 30 //25
const perPage = 150

/* La data desde la API de coingecko devuelve un valor del tipo *types.CoinsMarket
la informacion dentro es un json al que se le aplico unmarshal()
Peso aprox de todos los datos 28MB */
func GetMarketData(cg *cgClient.Client) ([]cgTypes.CoinMarketData, time.Time) {

	ids := []string{}
	vsCurrency := "usd"
	sparkline := true
	pcp := cgTypes.PriceChangePercentageObject
	priceChangePercentage := []string{pcp.PCP1h, pcp.PCP24h, pcp.PCP7d, pcp.PCP14d, pcp.PCP30d, pcp.PCP200d, pcp.PCP1y}
	order := cgTypes.OrderTypeObject.MarketCapDesc
	page := 1

	coinsList, err := cg.CoinsList(true)

	if err != nil {
		log.Fatal(err)
	}

	totalCoins := len(*coinsList)
	pagesToRequest := totalCoins / perPage

	var marketData []cgTypes.CoinMarketData

	if totalCoins%perPage > 0 {
		//Si quedan elementos sin traer traigo una pagina mas
		pagesToRequest++
	}

	for page < pagesToRequest {
		//Traer tanda de datos
		market, err := cg.CoinsMarket(vsCurrency, ids, order, perPage, page, sparkline, priceChangePercentage)
		if err != nil {
			fmt.Println(err)
			time.Sleep(time.Duration(time.Second * 60))
			continue
		}

		dataCount := len(*market)

		/* Matcheo las diferentes marketdatas con sus platforms */
		for i := 0; i < dataCount; i++ {

			for k := 0; k < totalCoins; k++ {

				if (*coinsList)[k].ID == (*market)[i].ID {
					var data cgTypes.CoinMarketData
					data.MarketData = (*market)[i]
					data.Platforms = make(map[string]string)
					for key, value := range (*coinsList)[k].Platforms {

						data.Platforms[key] = value

					}

					marketData = append(marketData, data)
					break
				}
			}
		}

		fmt.Println("Working on: ", page, "/", pagesToRequest)

		time.Sleep(time.Duration(time.Second * 60 / requestByMinute))
		page++
	}

	lastUpdate := time.Now()
	fmt.Println(len(marketData))
	fmt.Println(marketData[4])
	return marketData, lastUpdate
}
