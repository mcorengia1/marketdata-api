package sorting

import (
	cgTypes "api.jacarandapp.com/src/coingecko/types"
)

//Declaro el tipo de funcion que va a recibir la funcion de ordenamiento*/
type Order func(cgTypes.CoinMarketData, cgTypes.CoinMarketData) bool

func PriceAsc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.CurrentPrice > b.MarketData.CurrentPrice
}

func PriceDesc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.CurrentPrice < b.MarketData.CurrentPrice
}

func MarketCapAsc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.MarketCap > b.MarketData.MarketCap
}

func MarketCapDesc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.MarketCap < b.MarketData.MarketCap
}

func PriceChange24Asc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.PriceChange24h > b.MarketData.PriceChange24h
}

func PriceChange24Desc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.PriceChange24h < b.MarketData.PriceChange24h
}

func VolumeAsc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.TotalVolume > b.MarketData.TotalVolume
}

func VolumeDesc(a cgTypes.CoinMarketData, b cgTypes.CoinMarketData) bool {
	return a.MarketData.TotalVolume < b.MarketData.TotalVolume
}

/* Implementaciones siguientes:
cambiar el ordenamiento por bubujeo a una busqueda binaria
incluir orden por id */

func Reverse(array *[]cgTypes.CoinMarketData) {
	j := len(*array) - 1

	for i := 0; j-i > 1; i++ {

		(*array)[i], (*array)[j] = (*array)[j], (*array)[i]

		j--
	}
}

func SortBy(items *[]cgTypes.CoinMarketData, order Order) {
	var (
		n      = len(*items)
		sorted = false
	)

	for !sorted {
		swapped := false
		for i := 0; i < n-1; i++ {
			if order((*items)[i], (*items)[i+1]) {
				(*items)[i+1], (*items)[i] = (*items)[i], (*items)[i+1]
				swapped = true
			}
		}
		if !swapped {
			sorted = true
		}
		n = n - 1
	}
}
