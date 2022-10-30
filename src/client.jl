module Client

using JLD2
using Nettle
using JSON3
using PyCall
using DataStructures
using Dates
using DataFrames
np = pyimport("numpy")
include("utils.jl")
import ..TsDb: path_tsdb

dir_name(meta::AbstractDict) = return hexdigest("md5", JSON3.write(SortedDict(meta)))
f_path(dir_name::String, date::String) = return joinpath(path_tsdb, dir_name, date) * ".jld2"
f_path(meta::AbstractDict, date::String) = return joinpath(path_tsdb, dir_name(meta), date) * ".jld2"
dir_path(meta) = return joinpath(path_tsdb, dir_name(meta))

py"""
import numpy as np

def np_intersect1d(ar1, ar2, assume_unique=False, return_indices=False):
    # return zero based indexed arrays. Apply .+ 1
    return np.intersect1d(ar1, ar2, assume_unique=assume_unique, return_indices=return_indices)
"""

function query(meta::AbstractDict; start="", stop="9")::DataFrame
    # Get all matching metas
    dfs = []
    for meta_reg in matching_metas(meta)
        push!(dfs, fetch_df(dir_name(meta_reg), start=start, stop=stop))
    end

    if  length(dfs) === 0
        return DataFrame()
    elseif length(dfs) === 1
        return dfs[1]
    else
        return sort(outerjoin(dfs..., on="ts"), :ts)
    end
end

function query(key::String; start="", stop="9")::DataFrame
    # 1 dir_name, therefore definitnely returning 1 df only
    return fetch_df(key, start=start, stop=stop)
end

function fetch_df(key::String; start="", stop="9")::DataFrame
    dir = joinpath(path_tsdb, key)
    df = DataFrame()
    if !ispath(dir)
        println("$(dir) not found. Dropping meta...")
        drop(key)
    else
        mats = []
        for fn in readdir(dir)
            date = replace(fn, ".jld2" => "")
            if start <= date <= stop
                push!(mats, load(joinpath(dir, fn), "data"))
            end
        end
        df = DataFrame(vcat(mats...), ["ts", col_name(key2meta(key))])
    end
    return df
end

function col_name(meta::AbstractDict; 
    order=["measurement_name", "asset", "exchange", "col", "unit"]
    )::String

    tags = []

    for key in order
        if key in collect(keys(meta))
            push!(tags, meta[key])
        end
    end

    for (key, val) in pairs(SortedDict(meta))
        if key in order
            continue
        else
            push!(tags, val)
        end
    end
    return join(tags, "-")
end

function matching_metas(meta::AbstractDict)
    map = registry()
    matching = []
    for reg_meta in values(map)
        if all([key in string.(keys(reg_meta)) ? reg_meta[Symbol(key)] === val : false for (key, val) in pairs(meta)])
            push!(matching, reg_meta)
        end
    end
    return matching
end

function key2meta(key::String)
    map = registry()
    return map[Symbol(key)]
end

function upsert(meta::AbstractDict, m_incoming; assume_sorted=true)
    map_date2Ix = DefaultDict{AbstractString, Vector{Int}}(() -> Vector{Int}())
    for (i, ts) in enumerate(m_incoming[:, 1])
        push!(map_date2Ix[string(Date(ts))], i)
    end
    for (date, indices) in pairs(map_date2Ix)
        file_path = f_path(meta, date)
        in_partition = m_incoming[indices, :]
        
        if ispath(file_path)
            d0 = load(file_path, "data")
            if assume_sorted & (d0[end, 1] < in_partition[1, 1])
                d1 = vcat(d0, in_partition)
            else
                _, ix1, ix2 = py"np_intersect1d"(d0[:, 1], in_partition[:, 1], assume_unique=true, return_indices=true)
                d1 = vcat(d0[setdiff(1:size(d0)[1], ix1 .+ 1), :], in_partition)
            end
            d1 = d1[sortperm(d1[:, 1]), :]
        else
            d1 = in_partition
            register(meta)
        end
        save(file_path, Dict("data" => d1))
    end
end

function delete(meta::AbstractDict; start="", stop="9")
    dir = dir_path(meta)
    for fn in readdir(dir)
        date = replace(fn, ".jld2" => "")
        if start <= date <= stop
            rm(joinpath(dir, date))
        end
    end
end

function drop(meta::AbstractDict)
    deregister(dir_name(meta))
    rm(dir_path(meta), recursive=true)
end

function drop(key::String)
    deregister(key)
    rm(joinpath(path_tsdb, key), recursive=true)
end

function registry()::AbstractDict
    p = joinpath(path_tsdb, "registry.json")
    if !ispath(p)
        map = Dict()
    else
        map = copy(JSON3.read(read(p, String)))
    end
    return map
end

function register(meta::AbstractDict)
    map = registry()
    dir_key = dir_name(meta)
    if dir_key in keys(map)
        return
    else
        p = joinpath(path_tsdb, "registry.json")
        map[Symbol(dir_key)] = meta
        open(p, "w") do io
            JSON3.write(io, map)
        end
    end
end

function deregister(key:: String)
    p = joinpath(path_tsdb, "registry.json")
    map = registry()
    delete!(map, Symbol(key))
    open(p, "w") do io
        JSON3.write(io, map)
    end
end

function get_dates(meta::AbstractDict; min_max_only=false)
    for meta_reg in matching_metas(meta)
        println(meta_reg)
        dir = dir_path(meta_reg)
        println(dir)
        dates = []
        for fn in readdir(dir)
            date = replace(fn, ".jld2" => "")
            if !min_max_only
                println(date)
            end
            push!(dates, date)
        end
        println([minimum(dates), maximum(dates)])
        # if min_max_only
        #     return [minimum(dates), maximum(dates)]
        # else
        #     return dates
        # end
    end
end

function py_upsert(meta, py_ts, vec)
    v_ts=Nanosecond.(py_ts.astype(np.int64)) + DateTime(1970)
    upsert(
        meta,
        hcat(v_ts, vec)
    )
end
 
function py_query(meta; start="", stop="9")
    df = query(meta, start=string(start), stop=string(stop))
    # df = query(key, start=string(start), stop=string(stop))
    mat = Matrix(df)
    mat = ifelse.(mat.===missing, nothing, mat)
    return (names(df), mat)
end

end # Module

# query(Dict(
#         "measurement_name" => "trade bars",
#         "exchange" => "bitfinex",
#         "asset" => "ethusd",
#         "information" => "volume"
# ),
#     start="2022-02-07",
#     stop ="2022-02-13"
#     )