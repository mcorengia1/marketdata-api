package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	coinsInfo "api.jacarandapp.com/src/fetch/coins"
	fetchData "api.jacarandapp.com/src/fetch/marketdata"
	sorting "api.jacarandapp.com/src/sorting"

	cgClient "api.jacarandapp.com/src/coingecko/client"
	cgTypes "api.jacarandapp.com/src/coingecko/types"

	mongo "api.jacarandapp.com/src/controllers/mongo"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/adshao/go-binance/v2"

	"github.com/adrg/strutil"
	"github.com/adrg/strutil/metrics"
)

/* Market Data */
var marketData []cgTypes.CoinMarketData
var priceAsc, priceDesc, marketCapAsc, marketCapDesc, priceChange24Asc, priceChange24Desc, volumeAsc, volumeDesc []cgTypes.CoinMarketData

/* Reducted coin info */
var reductedCoinInfo []cgTypes.ReductedCoinInfo

/* Clients */
var cg *cgClient.Client

/* Flags */
var orderedCoinsReady = false

/* Error timer */
const waitOnErrors = 60

/* Loggers */
var logger log.Logger = log.NewLogfmtLogger(os.Stdout)

type exchangeReq struct {
	Key    string `json:"key" bson:"key"`
	Secret string `json:"secret" bson:"secret"`
	Limit  int    `json:"limit" bson:"limit"`
}

func updateReductedInfo() {

	ticker := time.NewTicker(24 * time.Hour)
	done := make(chan bool)

	reductedCoinInfo = coinsInfo.GetReductedCoinsInfo(mongo.Client)
	updateReductedMarketData()
	level.Info(logger).Log("msg", "Reducted Info synchronized with the database", "elements", len(reductedCoinInfo), "ts", log.DefaultTimestampUTC())

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			reductedCoinInfo = coinsInfo.GetReductedCoinsInfo(mongo.Client)
			updateReductedMarketData()

			level.Info(logger).Log("msg", "Reducted Info synchronized with the database", "elements", len(reductedCoinInfo), "ts", log.DefaultTimestampUTC())
		}
	}

}

func updateReductedMarketData() {

	reductedCoinInfoLen := len(reductedCoinInfo)
	marketDataLen := len(marketData)

	for i := 0; i < reductedCoinInfoLen; i++ {

		for k := 0; k < marketDataLen; k++ {

			if marketData[k].MarketData.ID == reductedCoinInfo[i].Id {

				//Hay una coincidencia entonces actualizo los valores de mercado
				reductedCoinInfo[i].CurrentPrice = marketData[k].MarketData.CurrentPrice
				reductedCoinInfo[i].SparkLine = marketData[k].MarketData.SparklineIn7d.Price
				reductedCoinInfo[i].PriceChangePercentage24h = marketData[k].MarketData.PriceChangePercentage24h

				/* comentar <<--------------- */
				// for key, value := range marketData[k].Platforms {

				// 	reductedCoinInfo[i].Platforms[key] = value

				// }
			}
		}
	}
}

func updateCoinInfo() {
	for {
		coinsInfo.UpdateCoinsInfo(cg, mongo.Client)
		level.Info(logger).Log("msg", "Coins info updated", "ts", log.DefaultTimestampUTC())
	}
}

func updateMarketData() {

	for {
		updateAllMarketData()
	}
}

func updateAllMarketData() {
	defer errManagment()

	marketData = fetchData.GetMarketData(cg)
	updateReductedMarketData()

	// Price
	sorting.SortBy(&marketData, sorting.PriceAsc)
	priceAsc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(priceAsc, marketData)

	sorting.Reverse(&marketData)
	priceDesc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(priceDesc, marketData)

	// MarketCap
	sorting.SortBy(&marketData, sorting.MarketCapAsc)
	marketCapAsc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(marketCapAsc, marketData)

	sorting.Reverse(&marketData)
	marketCapDesc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(marketCapDesc, marketData)

	// Price change 24hs
	sorting.SortBy(&marketData, sorting.PriceChange24Asc)
	priceChange24Asc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(priceChange24Asc, marketData)

	sorting.Reverse(&marketData)
	priceChange24Desc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(priceChange24Desc, marketData)

	// Total volume
	sorting.SortBy(&marketData, sorting.VolumeAsc)
	volumeAsc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(volumeAsc, marketData)

	sorting.Reverse(&marketData)
	volumeDesc = make([]cgTypes.CoinMarketData, len(marketData))
	copy(volumeDesc, marketData)

	level.Info(logger).Log("msg", "Market data updated", "elements", len(marketData), "ts", log.DefaultTimestampUTC())
	orderedCoinsReady = true
}

/* End market Data */

/* Handlers */

func getElementsById(elements *[]cgTypes.CoinMarketData, ids *[]string) []cgTypes.CoinMarketData {

	elementsLen := len(*elements)
	idsLen := len(*ids)

	var elementsById []cgTypes.CoinMarketData

	for i := 0; i < elementsLen; i++ {

		for k := 0; k < idsLen; k++ {

			if (*elements)[i].MarketData.ID == (*ids)[k] {
				elementsById = append(elementsById, (*elements)[i])
			}
		}
	}
	return elementsById
}

func coinsById(w http.ResponseWriter, r *http.Request) {
	/* Las peticiones de ids se hacen separadas por %2C que es
	el equivalente a una " , " cuando es recibido por la api */

	if orderedCoinsReady {
		//Que exista data para poder responder

		vars := mux.Vars(r)
		ids := strings.Split(vars["ids"], ",")

		//Entrego la informacion basica de esos ids
		elements := getElementsById(&marketData, &ids)
		json.NewEncoder(w).Encode(elements)

	}
}

func coinsByContract(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	contracts := strings.Split(vars["contracts"], ",")
	platform := vars["platform"]

	var elements []cgTypes.CoinMarketData

	marketdataCount := len(marketCapDesc)
	contractsCount := len(contracts)
	for i := 0; i < marketdataCount; i++ {

		for k := 0; k < contractsCount; k++ {

			if marketCapDesc[i].Platforms[platform] == contracts[k] {
				elements = append(elements, marketCapDesc[i])
			}
		}
	}
	json.NewEncoder(w).Encode(elements)
}

func completeCoinsInfoById(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	ids := strings.Split(vars["ids"], ",")
	var elements []cgTypes.CoinInfoDB

	idsLen := len(ids)
	for i := 0; i < idsLen; i++ {

		elements = append(elements, coinsInfo.GetCoinInfoById(mongo.Client, ids[i]))

	}

	json.NewEncoder(w).Encode(elements)
}

func completeCoinsInfoByContract(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	contracts := strings.Split(vars["contracts"], ",")

	var elements []cgTypes.CoinInfoDB

	idsLen := len(contracts)
	for i := 0; i < idsLen; i++ {

		elements = append(elements, coinsInfo.GetCoinInfoByContract(mongo.Client, contracts[i]))

	}

	json.NewEncoder(w).Encode(elements)
}

func reductedCoinsInfoByContract(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	contracts := strings.Split(vars["contracts"], ",")
	platform := vars["platform"]

	var elements []cgTypes.ReductedCoinInfo

	reductedInfoCount := len(reductedCoinInfo)
	contractsCount := len(contracts)

	for i := 0; i < contractsCount; i++ {

		for k := 0; k < reductedInfoCount; k++ {

			if reductedCoinInfo[k].Platforms[platform] == contracts[i] {
				elements = append(elements, reductedCoinInfo[k])
				break
			}
		}
	}
	json.NewEncoder(w).Encode(elements)
}

type exchangeAsset struct {
	Symbol      string
	BinanceName string
	CurrentInfo cgTypes.ReductedCoinInfo
	Holdings    [31]exchangeHolding
}

type exchangeHolding struct {
	Date   int64
	Free   string
	Locked string
	Price  float64
}

type exchangeResponse struct {
	Name       string
	Code       int
	Valuations []float32
	Profits    []float32
	Assets     []exchangeAsset
}

func binanceBalance(w http.ResponseWriter, r *http.Request) {
	// Get access token
	var exchange exchangeReq
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&exchange)

	client := binance.NewClient(exchange.Key, exchange.Secret)

	snapshot, err := client.NewGetAccountSnapshotService().Type("SPOT").Limit(30).Do(context.Background())
	allCoinsInfo, err := client.NewGetAllCoinsInfoService().Do(context.Background())

	var response exchangeResponse
	response.Name = "Binance"
	response.Code = snapshot.Code

	for i := len(snapshot.Snapshot) - 1; i > 0; i-- {
		//Recorro cada uno de los snapshot por dia

		var holding exchangeHolding
		holding.Date = snapshot.Snapshot[i].UpdateTime

		for k := 0; k < len(snapshot.Snapshot[i].Data.Balances); k++ {
			//Recorro cada balance de ese dia
			if snapshot.Snapshot[i].Data.Balances[k].Free > "0" || snapshot.Snapshot[i].Data.Balances[k].Locked > "0" {
				//Que tenga balance y no sea 0

				//[0] es hace un mes
				assetFound := false
				for j := 0; j < len(response.Assets); j++ {
					// Para cada balance recorro todos los balances ya guardados
					if snapshot.Snapshot[i].Data.Balances[k].Asset == response.Assets[j].Symbol {
						//ya existe tengo que append! response.asset[k].holdings[i]
						assetFound = true
						holding.Free = snapshot.Snapshot[i].Data.Balances[k].Free
						holding.Locked = snapshot.Snapshot[i].Data.Balances[k].Locked
						response.Assets[j].Holdings[len(snapshot.Snapshot)-i] = holding //Porque los guardo al reves
					}
				}
				if !assetFound {
					//No se encontro el asset entonces lo tengo que crear y agregar el holding
					var asset exchangeAsset
					asset.Symbol = snapshot.Snapshot[i].Data.Balances[k].Asset
					holding.Free = snapshot.Snapshot[i].Data.Balances[k].Free
					holding.Locked = snapshot.Snapshot[i].Data.Balances[k].Locked
					asset.Holdings[len(snapshot.Snapshot)-i] = holding //Porque los guardo al reves
					response.Assets = append(response.Assets, asset)
				}
			}
		}
	}

	// Match the responses values with the all coin info values
	for i := 0; i < len(allCoinsInfo); i++ {
		free, _ := strconv.ParseFloat(allCoinsInfo[i].Free, 64)
		locked, _ := strconv.ParseFloat(allCoinsInfo[i].Locked, 64)

		if free > 0 || locked > 0 {
			//Si hay un saldo recorro todo el response para encontrarlo

			var holding exchangeHolding
			holding.Date = time.Now().Unix()
			holding.Free = allCoinsInfo[i].Free
			holding.Locked = allCoinsInfo[i].Locked

			var found = false
			for k := 0; k < len(response.Assets); k++ {

				if response.Assets[k].Symbol == allCoinsInfo[i].Coin {
					found = true
					response.Assets[k].BinanceName = allCoinsInfo[i].Name
					response.Assets[k].Holdings[0] = holding
				}
			}

			if !found {
				//Tengo que agregar esta crypto que compraron hoy a los assets
				var asset exchangeAsset
				asset.BinanceName = allCoinsInfo[i].Name
				asset.Symbol = allCoinsInfo[i].Coin
				asset.Holdings[0] = holding

				response.Assets = append(response.Assets, asset)
			}
		}
	}

	//Get reducted info for each one and load prices for the last 7 days
	for i := 0; i < len(response.Assets); i++ {
		//Recorro cada asset
		var found = false
		for k := 0; k < len(reductedCoinInfo); k++ {
			//Para cada asset en ese dia

			if strings.EqualFold(reductedCoinInfo[k].Id, response.Assets[i].BinanceName) || strings.EqualFold(reductedCoinInfo[k].Name, response.Assets[i].BinanceName) ||
				strings.EqualFold(reductedCoinInfo[k].Id, response.Assets[i].Symbol) || strings.EqualFold(reductedCoinInfo[k].Name, response.Assets[i].Symbol) {
				//Likely match
				found = true
				response.Assets[i].CurrentInfo = reductedCoinInfo[k]
				break
			}
		}

		if !found {
			//Busco por symbol

			var matches []cgTypes.ReductedCoinInfo
			for k := 0; k < len(reductedCoinInfo); k++ {
				//Para cada asset en ese dia

				if strings.EqualFold(reductedCoinInfo[k].Symbol, response.Assets[i].Symbol) {
					//Symbol match I must check the names similarity
					matches = append(matches, reductedCoinInfo[k])
					found = true
				}
			}

			if len(matches) > 1 {
				//Si hubo mas de una coincidencia elijo la que tenga mayor string similarity
				fmt.Println("More than one coincidence")
				closestIndex := 0
				closestSimilarity := 0
				for k := 0; k < len(matches); k++ {
					//swg := metrics.NewSmithWatermanGotoh()
					swg := metrics.NewSmithWatermanGotoh()
					swg.CaseSensitive = false
					similarity := strutil.Similarity(reductedCoinInfo[k].Name, response.Assets[i].BinanceName, swg)

					if similarity > float64(closestSimilarity) {
						closestIndex = k
					}
				}
				response.Assets[i].CurrentInfo = matches[closestIndex]

			} else if len(matches) == 1 {
				response.Assets[i].CurrentInfo = matches[0]
			}
		}

		if len(response.Assets[i].CurrentInfo.SparkLine) > 0 {
			//Si hay coinData cargo los precios con la sparkline
			for k := 0; k < 7; k++ {
				var priceIndex = len(response.Assets[i].CurrentInfo.SparkLine) - 1 - k*(len(response.Assets[i].CurrentInfo.SparkLine)/7) //9,k=0 ** 9-6*(10/7)
				response.Assets[i].Holdings[k+1].Price = response.Assets[i].CurrentInfo.SparkLine[priceIndex]
			}
		}
		response.Assets[i].Holdings[0].Price = response.Assets[i].CurrentInfo.CurrentPrice
	}

	if err != nil {
		level.Error(logger).Log("msg", "Binance balance req error", "ts", log.DefaultTimestampUTC(), "err", err)
	}
	json.NewEncoder(w).Encode(response)
}

/* End Handlers */

/* Router logic */

func handleRequest(r *mux.Router) {

	//El routeo va en orden de la ruta mas especifica a la menos especifica

	r.HandleFunc("/coins/marketdata/ordered", orderedCoins).Queries("order", "{order}", "start", "{start:[0-9]+}", "end", "{end:[0-9]+}")

	r.HandleFunc("/coins/marketdata/ids", coinsById).Queries("ids", "{ids}")

	r.HandleFunc("/coins/marketdata/contracts", coinsByContract).Queries("platform", "{platform}", "contracts", "{contracts}")

	r.HandleFunc("/coins/info/complete/ids", completeCoinsInfoById).Queries("ids", "{ids}")

	r.HandleFunc("/coins/info/complete/contracts", completeCoinsInfoByContract).Queries("contracts", "{contracts}")

	r.HandleFunc("/coins/info/reducted/contracts", reductedCoinsInfoByContract).Queries("platform", "{platform}", "contracts", "{contracts}")

	r.HandleFunc("/balance/exchange/binance", binanceBalance)

	err := http.ListenAndServe(":10000", r)
	level.Error(logger).Log("msg", "Server handler error", "ts", log.DefaultTimestampUTC(), "err", err)
}

/*
Routing map:

/marketdata/ordered/coins/.. ====
/marketdata/ordered/exchanges/..
/marketdata/ordered/categories/..

/marketdata/ids/coins/basic|full/.. ====
/marketdata/historical/coins/..

*/

func orderedCoins(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	start, _ := strconv.Atoi(vars["start"])
	end, _ := strconv.Atoi(vars["end"])
	//No compruebo los errores antes porque ya compruebo los datos antes

	if end > len(marketData)-1 {
		end = len(marketData) - 1
	}

	switch vars["order"] {
	case "price-asc":
		json.NewEncoder(w).Encode(priceAsc[start:end])

	case "price-desc":
		json.NewEncoder(w).Encode(priceDesc[start:end])

	case "marketcap-asc":
		json.NewEncoder(w).Encode(marketCapAsc[start:end])

	case "marketcap-desc":
		json.NewEncoder(w).Encode(marketCapDesc[start:end])

	case "price-change-24-asc":
		json.NewEncoder(w).Encode(priceChange24Asc[start:end])

	case "price-change-24-desc":
		json.NewEncoder(w).Encode(priceChange24Desc[start:end])

	case "volume-asc":
		json.NewEncoder(w).Encode(volumeAsc[start:end])

	case "volume-desc":
		json.NewEncoder(w).Encode(volumeDesc[start:end])

	default:
		level.Error(logger).Log("msg", "Invalid requested order", "ts", log.DefaultTimestampUTC())
	}

}

func errManagment() {
	recoverInfo := recover()
	if recoverInfo != nil {
		fmt.Println(recoverInfo)

		fmt.Println("Waiting 60sec before continue with the requests")
		time.Sleep(time.Duration(time.Second * waitOnErrors))
	}
}

/* End router logic */

func main() {
	/* Coingecko client */
	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}
	cg = cgClient.NewClient(httpClient)

	go updateReductedInfo()
	go updateMarketData()
	go updateCoinInfo()

	router := mux.NewRouter()
	handleRequest(router)
}
