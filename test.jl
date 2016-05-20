include("main.jl")

type User
  name::AbstractString
  follows::Vector{Reference{User}}
end

type Review
  stars::UInt8
  user::Reference{User}
end

store = DB()

users = Table{User}(store)
reviews = Table{Review}(store)

jake = User("Jake", [])
push!(users, jake)
jeff = User("Jeff", [jake])
push!(users, jeff)
review = Review(5, jake)
push!(reviews, review)

@test reviews[1].stars == 5
@test deref(reviews[1].user, store).name == "Jake"
@test length(users) == 2
@test db_id[reviews[1]] == db_id[review]
@test db_id[deref(reviews[1].user, store)] == db_id[jake]
@test db_id[deref(users[2].follows[1], store)] == db_id[jake]
