package utils

import (
	cgTypes "api.jacarandapp.com/src/coingecko/types"
)

func GetElementsById(elements *[]cgTypes.CoinMarketData, ids *[]string) []cgTypes.CoinMarketData {

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
