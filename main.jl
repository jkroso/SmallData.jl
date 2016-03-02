@require "github.com/jkroso/abstract-ast.jl" => AST
@require "github.com/quinnj/SQLite.jl" => SQLite DB

const db_id = WeakKeyDict{Any,Int}()

table_exists(db::DB, T::DataType) = begin
  stmt = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='$T'"
  SQLite.query(db, stmt).data[1][1]|>get == 1
end

sqltype(::Type) = "BLOB"
sqltype(::Type{Void}) = "NULL"
sqltype{T<:AbstractString}(::Type{T}) = "TEXT"
sqltype{T<:Integer}(::Type{T}) = "INTEGER"
sqltype{T<:Real}(::Type{T}) = "REAL"
sqltype(t::TypeVar) = sqltype(t.ub)

abstract AbstractTable{T}
abstract Table{T} <: AbstractTable{T}
abstract TableView{T} <: AbstractTable{T}
immutable ValueTable{T} <: Table{T} db::DB end
immutable EntityTable{T} <: Table{T} db::DB end
immutable FilteredTable{T} <: TableView{T}
  table::AbstractTable{T}
  where::AbstractString
end

call{T}(::Type{Table{T}}, db::DB) = begin
  fields = fieldnames(T)
  types = map(fields) do name
    t = fieldtype(T, name)
    if t <: Vector && table_exists(db, eltype(t))
      Vector{UInt}
    elseif table_exists(db, t)
      UInt
    else
      t
    end
  end
  declarations = map((f, t) -> string('"', f, '"', ' ', sqltype(t)), fields, types)
  SQLite.execute!(db, "CREATE TABLE IF NOT EXISTS \"$T\" ($(join(declarations, ',')))")
  T.mutable ? EntityTable{T}(db) : ValueTable{T}(db)
end

Base.eltype{T}(::AbstractTable{T}) = T
Base.length(t::AbstractTable) =
  get(SQLite.query(db(t), "SELECT count(*) FROM \"$(name(t))\" $(where(t))").data[1][1], 0)
Base.endof(t::AbstractTable) = length(t)
width{T}(t::Table{T}) = nfields(T)
width(t::TableView) = width(t.table)

Base.start{T}(t::AbstractTable{T}) = begin
  stmt = SQLite.Stmt(db(t), sql(t))
  status = SQLite.execute!(stmt)
  stmt.handle, status
end
Base.done{T}(::AbstractTable{T}, state) = state[2] == SQLite.SQLITE_DONE
Base.next{T}(t::AbstractTable{T}, state) = begin
  handle, status = state
  status == SQLite.SQLITE_ROW || SQLite.sqliteerror(db(t))
  values = map(1:width(t)) do i
    juliatype = SQLite.juliatype(SQLite.sqlite3_column_type(handle, i))
    value = SQLite.sqlitevalue(juliatype, handle, i)
    coltype = nthfieldtype(i, T)
    if juliatype <: Integer && table_exists(db(t), coltype)
      getentity(Table{coltype}(db(t)), value)
    elseif coltype <: Vector && table_exists(db(t), eltype(coltype))
      elT = eltype(coltype)
      elT[getentity(Table{elT}(db(t)), id) for id in value]
    else
      value
    end
  end
  T(values...), (handle, SQLite.sqlite3_step(handle))
end

nthfieldtype(n::Int, T::DataType) = fieldtype(T, fieldnames(T)[n])

Base.next{T}(t::EntityTable{T}, state) = begin
  id = SQLite.sqlitevalue(Int, state[1], width(t) + 1)
  row, state = invoke(next, (AbstractTable{T}, Tuple), t, state)
  db_id[row] = id
  row, state
end

Base.summary{T}(t::AbstractTable{T}) = string(length(t), 'x', width(t), ' ', T, " Table")
Base.summary{T}(t::FilteredTable{T}) = string(length(t), 'x', width(t), ' ', T, " Table ", where(t))

Base.show{T}(io::IO, t::AbstractTable{T}) = begin
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
  values = map(fieldnames(T)) do c
    value = getfield(row, c)
    if isa(value, Vector) && table_exists(db(t), eltype(value))
      UInt[db_id[v] for v in value]
    elseif table_exists(db(t), typeof(value))
      db_id[value]
    else
      value
    end
  end
  params = join(repeated('?', width(t)), ',')
  SQLite.query(t.db, "INSERT INTO \"$T\" VALUES ($params)", values)
  t
end

Base.push!{T}(t::EntityTable{T}, row::T) = begin
  invoke(push!, (Table{T}, T), t, row)
  db_id[row] = get(SQLite.query(t.db, "SELECT last_insert_rowid() FROM \"$T\" LIMIT 1").data[1][1])
  t
end

Base.getindex(t::Table, i::Integer) = begin
  0 < i <= length(t) || throw(BoundsError(t, i))
  stmt = SQLite.Stmt(t.db, "$(sql(t)) LIMIT 1 OFFSET $(i - 1)")
  status = SQLite.execute!(stmt)
  next(t, (stmt.handle, status))[1]
end

Base.getindex(t::AbstractTable, r::UnitRange) = take(drop(t, r.start - 1), r.stop - r.start + 1)

Base.setindex!{T}(t::Table{T} , r::T, i::Integer) = begin
  0 < i <= length(t) || throw(BoundsError(t, i))
  fields = fieldnames(T)
  params = join(map(f -> string(f, "=?"), fields), ',')
  values = map(f -> getfield(r, f), fields)
  SQLite.query(t.db, "UPDATE \"$(name(t))\" SET $params LIMIT 1 OFFSET $(i - 1)", values)
end

getentity{T}(t::EntityTable{T}, i::Integer) = begin
  stmt = SQLite.Stmt(t.db, "SELECT *,rowid FROM \"$T\" WHERE rowid=$i LIMIT 1")
  status = SQLite.execute!(stmt)
  next(t, (stmt.handle, status))[1]
end

sql(t::AbstractTable) = "SELECT $(selection(t)) FROM \"$(name(t))\" $(where(t))"
sql(t::EntityTable) = "SELECT $(selection(t)),rowid FROM \"$(name(t))\" $(where(t))"
selection(::AbstractTable) = "*"
name{T}(::AbstractTable{T}) = string(T)
db(t::TableView) = db(t.table)
db(t::Table) = t.db

immutable TableReference <: AST.AST
  name::Symbol
end

Base.filter(f::Function, t::Table) = begin
  ast = AST.getAST(f)
  env = AST.getEnv(f)
  fn = AST.simplify(ast, env)
  env[fn.params[1].name] = TableReference(symbol(name(t)))
  try
    FilteredTable(t, sql(fn.body.value, env))
  catch
    invoke(filter, (Function,Any), f, t)
  end
end

where(t::AbstractTable) = ""
where(t::FilteredTable) = string("WHERE ", t.where)

sql(a::AST.Call, env::AST.Env) = begin
  if a.callee.name == :getfield && isa(env[a.args[1].name], TableReference)
    sql(a.args[2], env)
  elseif haskey(sqlfunctions, a.callee.name)
    args = map(a -> sql(a, env), a.args)
    string(args[1], ' ', sql(a.callee, env), ' ', args[2])
  else
    sql(AST.interpret(a, env))
  end
end
sql(a::AST.GlobalReference, env::AST.Env) = sqlfunctions[a.name]
sql(a::AST.LocalReference, env::AST.Env) = string('"', a.name, '"')
sql(a::AST.Literal, env::AST.Env) = sql(a.value)
sql(s::AbstractString) = string('\'', s, '\'')
sql(s::Any) = string(s)

const sqlfunctions = Dict(symbol("==") => symbol("="))
