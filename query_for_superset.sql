select currency
	,toDateTime(open_time) open_time
	,argMax(close_price, dt_load) close
	,argMax(high_price, dt_load) high_price
	,argMax(low_price, dt_load) low_price
from default.CryptoCurrencyPriceUSD
group by currency, open_time
order by currency, open_time
;