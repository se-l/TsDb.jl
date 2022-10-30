module TsDb

import YAML: load_file

export Client, Server

cfg = load_file(joinpath(@__FILE__, "..", "config.yaml"))
const path_tsdb = cfg["path_tsdb"]
const path_ui = cfg["path_ui"]

include("client.jl")
include("server.jl")

# precompile()

end  # module TsDb