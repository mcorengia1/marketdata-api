package fetch

import (
	"os"
	"time"

	cgClient "api.jacarandapp.com/src/coingecko/client"
	cgTypes "api.jacarandapp.com/src/coingecko/types"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var logger log.Logger = log.NewLogfmtLogger(os.Stdout)

const requestByMinute = 25
const perPage = 150

/* La data desde la API de coingecko devuelve un valor del tipo *types.CoinsMarket
la informacion dentro es un json al que se le aplico unmarshal()
Peso aprox de todos los datos 28MB */
func GetMarketData(cg *cgClient.Client, currentMktData []cgTypes.CoinMarketData) []cgTypes.CoinMarketData {

	ids := []string{}
	vsCurrency := "usd"
	sparkline := true
	pcp := cgTypes.PriceChangePercentageObject
	priceChangePercentage := []string{pcp.PCP1h, pcp.PCP24h, pcp.PCP7d, pcp.PCP14d, pcp.PCP30d, pcp.PCP200d, pcp.PCP1y}
	order := cgTypes.OrderTypeObject.MarketCapDesc
	page := 1

	newMarketData := make([]cgTypes.CoinMarketData, len(currentMktData))
	copy(newMarketData, currentMktData)

	coinsList, err := cg.CoinsList(true)

	if err != nil {
		level.Error(logger).Log("msg", "Cannot get coin list in GetMarketData", "ts", log.DefaultTimestampUTC(), "err", err)
	}

	totalCoins := len(*coinsList)
	level.Info(logger).Log("msg", "Coins list received", "items", totalCoins, "ts", log.DefaultTimestampUTC())

	pagesToRequest := totalCoins / perPage

	if totalCoins%perPage > 0 {
		//Si quedan elementos sin traer traigo una pagina mas
		pagesToRequest++
	}

	for page < pagesToRequest {
		//Traer tanda de datos
		market, err := cg.CoinsMarket(vsCurrency, ids, order, perPage, page, sparkline, priceChangePercentage)

		if err != nil {
			level.Error(logger).Log("msg", "Cannot get market info in GetMarketData, waiting to retry", "ts", log.DefaultTimestampUTC, "err", err)
			time.Sleep(time.Duration(time.Second * 60))
			continue
		}

		dataCount := len(*market)

		/* Matcheo las diferentes marketdatas con sus platforms */
		for i := 0; i < dataCount; i++ {

			var found = false
			var data cgTypes.CoinMarketData

			for k := 0; k < len(newMarketData); k++ {
				// Para cada elemento recibido actualizo el anterior o si no esta lo agrego

				if (*market)[i].ID == newMarketData[k].MarketData.ID {
					found = true
					newMarketData[k].MarketData = (*market)[i]

					data.Platforms = make(map[string]string)
					for key, value := range (*coinsList)[i].Platforms {
						data.Platforms[key] = value
					}
					newMarketData[k].Platforms = data.Platforms

					break
				}
			}

			if !found {
				//Nuevo elemento lo tengo que agregar
				data.MarketData = (*market)[i]
				data.Platforms = make(map[string]string)

				for key, value := range (*coinsList)[i].Platforms {
					data.Platforms[key] = value
				}
				newMarketData = append(newMarketData, data)
			}
		}

		completed := page * 100 / pagesToRequest

		switch completed {
		case 25:
			level.Info(logger).Log("msg", "MarketData updating 25%% complete", "ts", log.DefaultTimestampUTC())
		case 50:
			level.Info(logger).Log("msg", "MarketData updating 50%% complete", "ts", log.DefaultTimestampUTC())
		case 75:
			level.Info(logger).Log("msg", "MarketData updating 75%% complete", "ts", log.DefaultTimestampUTC())
		}

		time.Sleep(time.Duration(time.Second * 60 / requestByMinute))
		page++
	}

	return newMarketData
}

// package fetch

// import (
// 	"os"
// 	"time"

// 	cgClient "api.jacarandapp.com/src/coingecko/client"
// 	cgTypes "api.jacarandapp.com/src/coingecko/types"

// 	"github.com/go-kit/log"
// 	"github.com/go-kit/log/level"
// )

// var logger log.Logger = log.NewLogfmtLogger(os.Stdout)

// const requestByMinute = 25
// const perPage = 150

// /* La data desde la API de coingecko devuelve un valor del tipo *types.CoinsMarket
// la informacion dentro es un json al que se le aplico unmarshal()
// Peso aprox de todos los datos 28MB */
// func GetMarketData(cg *cgClient.Client, currentMktData *[]cgTypes.CoinMarketData) []cgTypes.CoinMarketData {

// 	ids := []string{}
// 	vsCurrency := "usd"
// 	sparkline := true
// 	pcp := cgTypes.PriceChangePercentageObject
// 	priceChangePercentage := []string{pcp.PCP1h, pcp.PCP24h, pcp.PCP7d, pcp.PCP14d, pcp.PCP30d, pcp.PCP200d, pcp.PCP1y}
// 	order := cgTypes.OrderTypeObject.MarketCapDesc
// 	page := 1

// 	coinsList, err := cg.CoinsList(true)

// 	if err != nil {
// 		level.Error(logger).Log("msg", "Cannot get coin list in GetMarketData", "ts", log.DefaultTimestampUTC(), "err", err)
// 	}

// 	totalCoins := len(*coinsList)
// 	level.Info(logger).Log("msg", "Coins list received", "items", totalCoins, "ts", log.DefaultTimestampUTC())

// 	pagesToRequest := totalCoins / perPage

// 	var marketData []cgTypes.CoinMarketData

// 	if totalCoins%perPage > 0 {
// 		//Si quedan elementos sin traer traigo una pagina mas
// 		pagesToRequest++
// 	}

// 	for page < pagesToRequest {
// 		//Traer tanda de datos
// 		market, err := cg.CoinsMarket(vsCurrency, ids, order, perPage, page, sparkline, priceChangePercentage)

// 		if err != nil {
// 			level.Error(logger).Log("msg", "Cannot get market info in GetMarketData, waiting to retry", "ts", log.DefaultTimestampUTC, "err", err)
// 			time.Sleep(time.Duration(time.Second * 60))
// 			continue
// 		}

// 		dataCount := len(*market)

// 		/* Matcheo las diferentes marketdatas con sus platforms */
// 		for i := 0; i < dataCount; i++ {

// 			for k := 0; k < len(*currentMktData); k++ {
// 				// Para cada elemento recibido actualizo el anterior o si no esta lo agrego

// 				if (*market)[i].ID == (*currentMktData)[k].MarketData.ID {

// 				}
// 			}

// 			for k := 0; k < totalCoins; k++ {

// 				if (*coinsList)[k].ID == (*market)[i].ID {
// 					var data cgTypes.CoinMarketData
// 					data.MarketData = (*market)[i]
// 					data.Platforms = make(map[string]string)
// 					for key, value := range (*coinsList)[k].Platforms {

// 						data.Platforms[key] = value
// 					}

// 					marketData = append(marketData, data)
// 					break
// 				}
// 			}
// 		}

// 		completed := page * 100 / pagesToRequest

// 		switch completed {
// 		case 25:
// 			level.Info(logger).Log("msg", "MarketData updating 25%% complete", "ts", log.DefaultTimestampUTC())
// 		case 50:
// 			level.Info(logger).Log("msg", "MarketData updating 50%% complete", "ts", log.DefaultTimestampUTC())
// 		case 75:
// 			level.Info(logger).Log("msg", "MarketData updating 75%% complete", "ts", log.DefaultTimestampUTC())
// 		}

// 		time.Sleep(time.Duration(time.Second * 60 / requestByMinute))
// 		page++
// 	}

// 	return marketData
// }
