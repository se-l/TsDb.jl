using Dates
import TsDb: Client

CODECOV_TOKEN=364e8556-8f66-4713-9545-adce5ea75560

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

meta=Dict(
    # "measurement_name"=> "order book",
    # "measurement_name"=> "trade bars",
    "measurement_name"=> "label",
    # "asset"=> "adausd", # adausd
    # "exchange" => "bitfinex",
    # "information"=> "return_attribution_sample_weights",
    # "level_from_price_haircut"=> 0.001,
)
println(Client.get_dates(meta, min_max_only=true))
start="2022-02-07"
stop="2022-02-28"
# df = query(meta, start=start, stop=stop)
start = Date(2022, 2, 7)
end_ = Date(2022, 4, 19)
for m in matching_metas(meta)
    # delete(m, start=string(start), stop=string(stop))
    # drop(m)
    println(keys(m))
end
    if Symbol("unit_size") in keys(m)
        # println(m)
        continue
    else
        # drop(m)
        println(m)
    end
end
