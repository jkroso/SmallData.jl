include("main.jl")

type User
  name::AbstractString
end

type Review
  stars::UInt8
  user::User
end

store = DB()

users = Table{User}(store)
reviews = Table{Review}(store)

jake = User("Jake")
review = Review(5, jake)
push!(users, jake)
push!(reviews, review)

@test reviews[1].stars == 5
@test reviews[1].user.name == "Jake"
@test length(users) == 1
@test db_id[reviews[1]] = db_id[review]
@test db_id[reviews[1].user] = db_id[jake]
