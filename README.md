# RDD — Replaceable Database Driver para Sea

O **RDD** é uma camada de abstração de dados e ORM/ODM para a linguagem **Sea**, projetada sob o conceito de *Replaceable Database Driver*: você define suas estruturas de dados (planas ou aninhadas como documentos) e escreve suas queries de forma única, independente de onde os dados serão persistidos (PostgreSQL, SQLite, SQL Server, CockroachDB, MongoDB, S3, etc.).

O RDD cuida de:
1. **Registro e validação de contrato de drivers** (`rdd.register_driver`).
2. **Definição e validação de Schemas** (`rdd.schema`), suportando tanto modelos tabulares quanto documentos encadeados (`document`, `array` de sub-documentos, `map`, etc.), valores padrão (`default_val`), campos gerados (`auto`) e validação de tipos.
3. **Query Builder Fluente** (`where`, `or_where`, `where_in`, `where_between`, `fields`, `order_by`, `limit`, `offset`), gerando um **Query Plan (AST)** abstrato que é repassado junto com o `Schema` e o documento bruto para o driver registrado.

---

## 1. Definindo Estruturas (Schemas)

```sea
local rdd = require("./rdd")

local Endereco = rdd.schema("enderecos", {
    rua = {type = "string", required = true},
    numero = "int",
    cidade = {type = "string", required = true},
    uf = {type = "string", default_val = "SP", min = 2, max = 2},
})

local Clientes = rdd.schema("clientes", {
    id = {type = "int", primary_key = true, auto = true},
    nome = {type = "string", required = true, min = 2},
    email = {type = "string", unique = true},
    ativo = {type = "bool", default_val = true},
    saldo = {type = "number", default_val = 0.0},
    endereco = Endereco, -- sub-documento encadeado (ou inline sem 'type')
    tags = {type = "array", items = "string"},
    contatos = {
        type = "array",
        items = {
            tipo = {type = "string", required = true},
            valor = {type = "string", required = true},
        },
    },
})
```

Tipos suportados: `"string"`, `"int"`, `"number"`, `"bool"`, `"bytes"`, `"time"`, `"uuid"`, `"map"`, `"any"`, `"document"`, `"array"`.

---

## 2. Manipulação e Queries Abstratas

```sea
local db = rdd.open("meu_driver", {uri = "..."})?
defer db:close()

local _ = db:migrate(Clientes)?
local clientes = db:model(Clientes)

-- Inserção (com validação de tipos e documentos aninhados)
local novo = clientes:insert({
    nome = "Maria Silva",
    email = "maria@example.com",
    endereco = {
        rua = "Av. Paulista",
        numero = 1000,
        cidade = "São Paulo",
    },
    tags = ["vip", "varejo"],
    contatos = [
        {tipo = "celular", valor = "11999999999"},
    ],
})?

-- Queries fluentes independentes do banco (incluindo dot-notation em sub-documentos)
local lista = clientes:where("ativo", true)
    :where("saldo", ">=", 100)
    :where("endereco.uf", "SP")
    :or_where(function(sub)
        sub:where("endereco.uf", "RJ"):where_in("nome", ["Ana", "Bia"])
    end)
    :order_by("-saldo")
    :limit(20)
    :offset(0)
    :all()?

-- Atualização parcial (suporta campos aninhados via dot-path)
local afetados = clientes:where("id", novo.id):update({
    saldo = 500.0,
    ["endereco.cidade"] = "Campinas",
})?

-- Remoção
local removidos = clientes:where("ativo", false):delete()?
```

---

## 3. Contrato para Registro de Drivers

Qualquer driver (SQL, NoSQL ou Object Storage) pode ser plugado registrando uma implementação com `rdd.register_driver(nome, driver)`:

```sea
rdd.register_driver("nome_do_driver", {
    open = function(opts)
        local conn = {}

        -- Obrigatórios:
        function conn:insert(schema, doc) ... end          -- → doc_inserido | error
        function conn:find(schema, plan) ... end           -- → array de docs | error
        function conn:update(schema, plan, changes) ... end -- → qtd_atualizada (int) | error
        function conn:delete(schema, plan) ... end         -- → qtd_removida (int) | error
        function conn:close() ... end                      -- → true | nil | error

        -- Opcionais (o RDD possui fallback caso não implementados):
        function conn:migrate(schema) ... end              -- → true | error
        function conn:insert_many(schema, docs) ... end    -- → array de docs | error
        function conn:count(schema, plan) ... end          -- → int | error
        function conn:transaction(fn) ... end              -- → fn(tx_conn) | error

        return conn
    end,
})
```

### Estrutura do `plan` (Query Plan AST) entregue ao Driver

```sea
{
    collection = "clientes",
    schema     = <Schema>,
    fields     = ["id", "nome", "endereco.cidade"], -- vazio [] = todos os campos
    filters    = [
        {kind = "cond", logic = "and", field = "ativo", op = "=", value = true},
        {kind = "cond", logic = "and", field = "endereco.uf", op = "=", value = "SP"},
        {
            kind = "group",
            logic = "or",
            filters = [
                {kind = "cond", logic = "and", field = "endereco.uf", op = "=", value = "RJ"},
            ],
        },
    ],
    orders     = [{field = "saldo", dir = "desc"}],
    limit      = 20,
    offset     = 0,
}
```

---

## 4. Executando os Testes

```sh
~/projects/sea_vm/bin/sea check *.sea
~/projects/sea_vm/bin/sea test -v
```
