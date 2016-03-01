include("main.jl")

type User
  name::AbstractString
  follows::Vector{User}
end

type Review
  stars::UInt8
  user::User
end

store = DB()

users = Table{User}(store)
reviews = Table{Review}(store)

jake = User("Jake", [])
jeff = User("Jeff", [jake])
review = Review(5, jake)
push!(users, jake, jeff)
push!(reviews, review)

@test reviews[1].stars == 5
@test reviews[1].user.name == "Jake"
@test length(users) == 2
@test db_id[reviews[1]] == db_id[review]
@test db_id[reviews[1].user] == db_id[jake]
@test db_id[users[2].follows[1]] == db_id[jake]
