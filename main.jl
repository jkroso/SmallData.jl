import SQLite
const DB = SQLite.DB

sqltype(::Type) = "BLOB"
sqltype(::Type{Void}) = "NULL"
sqltype{T<:AbstractString}(::Type{T}) = "TEXT"
sqltype{T<:Integer}(::Type{T}) = "INTEGER"
sqltype{T<:Real}(::Type{T}) = "REAL"
sqltype(t::TypeVar) = sqltype(t.ub)

type Table{T}
  db::DB
  width::UInt8
end

call{T}(::Type{Table{T}}, db::DB) = begin
  fields = fieldnames(T)
  types = map(s->fieldtype(T, s), fields)
  decs = [string(f, ' ', sqltype(t)) for (f, t) in zip(fields, types)]
  SQLite.execute!(db, "CREATE TABLE IF NOT EXISTS \"$T\" ($(join(decs, ',')))")
  Table{T}(db, length(fields))
end

Base.eltype{T}(::Table{T}) = T
Base.length{T}(t::Table{T}) = SQLite.query(t.db, "SELECT count(*) from \"$T\"").data[1][1] |> get
Base.endof(t::Table) = length(t)

Base.start{T}(t::Table{T}) = begin
  stmt = SQLite.Stmt(t.db, sql(t))
  status = SQLite.execute!(stmt)
  stmt.handle, status
end
Base.done{T}(::Table{T}, state) = state[2] == SQLite.SQLITE_DONE
Base.next{T}(t::Table{T}, state) = begin
  handle, status = state
  status == SQLite.SQLITE_ROW || SQLite.sqliteerror(db(t))
  values = map(1:t.width) do i
    juliatype = SQLite.juliatype(SQLite.sqlite3_column_type(handle, i))
    SQLite.sqlitevalue(juliatype, handle, i)
  end
  T(values...), (handle, SQLite.sqlite3_step(handle))
end

Base.summary{T}(t::Table{T}) = string(length(t), 'x', t.width, ' ', T, " Table")

Base.show{T}(io::IO, t::Table{T}) = begin
  println(io, summary(t))
  fields = fieldnames(T)
  rows = map(row -> map(f -> row.(f), fields), t)
  showrows(io, rows, map(string, fields))
end

showrows(io::IO, rows::AbstractArray, headers::AbstractArray) = begin
  rows = map(r -> map(repr, r), rows)
  widths = map(eachindex(headers)) do i
    column = map(r -> strwidth(r[i]), rows)
    reduce(max, strwidth(headers[i]), column)
  end
  showrow(io, headers, widths)
  println(io, '|', repeat("-", sum(widths) + 3 * length(widths) - 1), '|')
  for row in rows
    showrow(io, row, widths)
  end
end

showrow(io::IO, row::AbstractArray, widths::AbstractArray) = begin
  for (item, width) in zip(row, widths)
    print(io, "| ", lpad(item, width), ' ')
  end
  println(io, '|')
end

Base.push!{T}(t::Table{T}, row::T) = begin
  columns = fieldnames(T)
  values = map(c -> row.(c), columns)
  params = join(repeated('?', length(columns)), ',')
  SQLite.query(t.db, "INSERT INTO \"$T\" VALUES ($params)", values)
  t
end

Base.getindex(t::Table, i::Integer) = begin
  0 < i <= length(t) || throw(BoundsError(t, i))
  stmt = SQLite.Stmt(t.db, "$(sql(t)) LIMIT 1 OFFSET $(i - 1)")
  status = SQLite.execute!(stmt)
  next(t, (stmt.handle, status))[1]
end

Base.getindex(t::Table, r::UnitRange) = take(drop(t, r.start - 1), r.stop - r.start + 1)

Base.setindex!{T}(t::Table{T}, r::T, i::Integer) = begin
  0 < i <= length(t) || throw(BoundsError(t, i))
  fields = fieldnames(T)
  params = join(map(f -> string(f, "=?"), fields), ',')
  values = map(f -> r.(f), fields)
  SQLite.query(t.db, "UPDATE \"$T\" SET $params LIMIT 1 OFFSET $(i - 1)", values)
end

sql{T}(t::Table{T}) = "SELECT * FROM \"$T\""
