package reducted

import (
	"os"
	"time"

	"api.jacarandapp.com/src/controllers/mongo"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	cgTypes "api.jacarandapp.com/src/coingecko/types"
	coinsInfo "api.jacarandapp.com/src/fetch/coins"
)

/* Loggers */
var logger log.Logger = log.NewLogfmtLogger(os.Stdout)

func UpdateReductedInfo(reductedCoinInfo *[]cgTypes.ReductedCoinInfo, mktData *[]cgTypes.CoinMarketData) {

	ticker := time.NewTicker(24 * time.Hour)
	done := make(chan bool)

	*reductedCoinInfo = coinsInfo.GetReductedCoinsInfo(mongo.Client)
	updateReductedMarketData(mktData, reductedCoinInfo)
	level.Info(logger).Log("msg", "Reducted Info synchronized with the database", "elements", len(*reductedCoinInfo), "ts", log.DefaultTimestampUTC())

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			*reductedCoinInfo = coinsInfo.GetReductedCoinsInfo(mongo.Client)
			updateReductedMarketData(mktData, reductedCoinInfo)

			level.Info(logger).Log("msg", "Reducted Info synchronized with the database", "elements", len(*reductedCoinInfo), "ts", log.DefaultTimestampUTC())
		}
	}

}

func updateReductedMarketData(mktData *[]cgTypes.CoinMarketData, reductedCoinInfo *[]cgTypes.ReductedCoinInfo) {

	for i := 0; i < len(*reductedCoinInfo); i++ {

		for k := 0; k < len(*mktData); k++ {

			if (*mktData)[k].MarketData.ID == (*reductedCoinInfo)[i].Id {

				//Hay una coincidencia entonces actualizo los valores de mercado
				(*reductedCoinInfo)[i].CurrentPrice = (*mktData)[k].MarketData.CurrentPrice
				(*reductedCoinInfo)[i].SparkLine = (*mktData)[k].MarketData.SparklineIn7d.Price
				(*reductedCoinInfo)[i].PriceChangePercentage24h = (*mktData)[k].MarketData.PriceChangePercentage24h

			}
		}
	}
}
