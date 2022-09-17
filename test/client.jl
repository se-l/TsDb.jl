module TestClient

using Test, Dates
import TsDb: Client

const meta = Dict("a"=>1, "b"=>2)

@testset "deregister" begin    
    Client.register(meta)
    Client.deregister(Client.dir_name(meta))
end

@testset "upsert" begin    
    Client.register(meta)
    Client.upsert(meta, [[DateTime(2022, 1, i, 1, 1, i) for i in 1:3] [2,3,4]])
    Client.upsert(meta, [[DateTime(2022, 1, 1, 1, 1, i) for i in 3:5] [2,3,4]])
    Client.deregister(Client.dir_name(meta))
end

@testset "query" begin    
    Client.upsert(meta, [[DateTime(2022, 1, i, 1, 1, i) for i in 1:3] [2,3,4]])
    Client.query(meta)
    Client.drop(meta)
end

# @testset "query" begin    
#     Client.upsert(meta, [[DateTime(2022, 1, i, 1, 1, i) for i in 1:3] [2,3,4]])
#     Client.delete(meta)
#     Client.drop(meta)
# end

@testset "matching metas" begin    
    Client.upsert(Dict("a"=>1, "b"=>2), [[DateTime(2022, 1, i, 1, 1, i) for i in 1:3] [2,3,4]])
    Client.upsert(Dict("a"=>1), [[DateTime(2022, 1, i, 1, 1, i) for i in 1:3] [2,3,4]])
    @test length(Client.matching_metas(Dict("a"=>1))) == 2
    Client.drop(meta)
    Client.drop((Dict("a"=>1)))
end

end # module
