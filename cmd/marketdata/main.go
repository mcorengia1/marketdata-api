package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
)

/* Market Data */
var marketData []cgTypes.CoinMarketData
var priceAsc, priceDesc, marketCapAsc, marketCapDesc, priceChange24Asc, priceChange24Desc, volumeAsc, volumeDesc []cgTypes.CoinMarketData
var lastUpdate time.Time

var cg *cgClient.Client

/* Flags */
var orderedCoinsReady = false

func updateCoinInfo() {
	for {
		coinsInfo.UpdateCoinsInfo(cg, mongo.Client)
	}
}

func updateMarketData() {

	for {
		marketData, lastUpdate = fetchData.GetMarketData(cg)

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

		fmt.Println("Data updated on ", lastUpdate)
		orderedCoinsReady = true
	}
}

/* End market Data */

/* Ids handler */

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

func coinsByIdHandler(w http.ResponseWriter, r *http.Request) {
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

/* End Id handler */

func coinsByContractHandler(w http.ResponseWriter, r *http.Request) {

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

func coinsInfoByIdsHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	ids := strings.Split(vars["ids"], ",")
	var elements []cgTypes.CoinInfoDB

	idsLen := len(ids)
	for i := 0; i < idsLen; i++ {

		elements = append(elements, coinsInfo.GetCoinInfoById(mongo.Client, ids[i]))

	}

	json.NewEncoder(w).Encode(elements)
}

/* Router logic */

func handleRequest(r *mux.Router) {

	//El routeo va en orden de la ruta mas especifica a la menos especifica

	r.HandleFunc("/coins/marketdata/ordered", orderedCoinsHandler).Queries("order", "{order}", "start", "{start:[0-9]+}", "end", "{end:[0-9]+}")

	r.HandleFunc("/coins/marketdata/ids", coinsByIdHandler).Queries("ids", "{ids}")

	r.HandleFunc("/coins/marketdata/contracts", coinsByContractHandler).Queries("platform", "{platform}", "contracts", "{contracts}")

	r.HandleFunc("/coins/info/ids", coinsInfoByIdsHandler).Queries("ids", "{ids}")

	log.Fatal(http.ListenAndServe(":10000", r))
}

/*
Routing map:

/marketdata/ordered/coins/.. ====
/marketdata/ordered/exchanges/..
/marketdata/ordered/categories/..

/marketdata/ids/coins/basic|full/.. ====
/marketdata/historical/coins/..

*/

func orderedCoinsHandler(w http.ResponseWriter, r *http.Request) {

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
		fmt.Println("Error")
	}

}

/* End router logic */

func main() {
	/* Coingecko client */
	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}
	cg = cgClient.NewClient(httpClient)

	go updateMarketData()
	go updateCoinInfo()

	router := mux.NewRouter()
	handleRequest(router)
}
