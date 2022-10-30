module Server

using JSON3
import ..Client: matching_metas, query
import ..TsDb: path_ui
using Genie, Genie.Router, Genie.Requests, Genie.Renderer.Json

function start(port::Int=8000)
    Genie.config.run_as_server = true
    Genie.config.cors_headers["Access-Control-Allow-Origin"] = "http://localhost:7202"
    Genie.config.cors_headers["Access-Control-Allow-Headers"] = "Content-Type"
    Genie.config.cors_headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    Genie.config.cors_headers["Access-Control-Allow-Credentials"] = "true"
    Genie.config.cors_allowed_origins = ["*"]

    println(Genie.config.cors_headers)

    route("/view/list", method = GET) do 
        views = []
        for (root, dirs, files) = walkdir(joinpath(path_ui, "views"))
            for file in files
                push(views, file)
            end
        end
        views |> json
    end

    route("/view/get", method = POST) do 
        payload = jsonpayload()
        p = joinpath(path_ui, "views", payload.get("name"))
        JSON3.read(read(p, String)) |> json
    end

    route("/view/save", method = POST) do 
        payload = jsonpayload()
        p = joinpath(path_ui, "views", payload.get("name"))
        open(p, "w") do io
            JSON3.write(io, payload)
        end
    end

    route("/view/delete", method = POST) do 
        payload = jsonpayload()
        joinpath(path_ui, "views", payload.get("name")) |> rm
    end

    route("/data/run") do
        # Jl code to exec ?
    end

    route("/data/save") do
        # datasets for future ref..
    end

    route("/query/meta", method = POST) do 
        # All metas matching meta. Like view/list ...
        @show jsonpayload()
        @show rawpayload()
        meta = jsonpayload()
        query(matching_metas(meta)) |> json
    end

    route("/query/:path") do 
        # Spefic datasets. like data/add. first pick meta, then load actual data
        dir_name = payload(:path)
        println("Fetching $(path)")
        query(dir_name) |> json
    end

    up(port)
end

end
