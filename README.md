# RDD — Replaceable Database Driver para Sea

O **RDD** é uma camada de abstração de dados e ORM/ODM para a linguagem **Sea**, projetada sob o conceito de *Replaceable Database Driver*: você define suas estruturas de dados (planas ou aninhadas como documentos) e escreve suas queries de forma única, independente de onde os dados serão persistidos (PostgreSQL, SQLite, SQL Server, CockroachDB, MongoDB, S3, etc.).

O RDD cuida de:
1. **Registro e validação de contrato de drivers** (`rdd.register_driver`).
2. **Definição e validação de Schemas** (`rdd.schema`), suportando tanto modelos tabulares quanto documentos encadeados (`document`, `array` de sub-documentos, `map`, etc.), valores padrão (`default_val`), campos gerados (`auto`) e validação de tipos.
3. **Query Builder Fluente** (`where`, `or_where`, `where_in`, `where_between`, `fields`, `order_by`, `limit`, `offset`, `group_by`/`aggregate`, `paginate`), gerando um **Query Plan (AST)** abstrato que é repassado junto com o `Schema` e o documento bruto para o driver registrado.

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

-- Condição escrita como expressão: os valores entram como `?` (na ordem) e os
-- campos são validados pelo schema; vira a mesma árvore do plano
local pedidos = clientes:where("(endereco.uf = ? or endereco.uf = ?) and saldo >= ?", "SP", "RJ", 100)
    :order_by("nome")
    :all()?
```

Na expressão: `campo op valor` com `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `like`, `ilike`, `contains`, `starts_with`, `ends_with`; `campo [not] in ('a', 'b')` ou `campo [not] in ?` (um array); `campo between 1 and 9`; `campo is [not] null`. Valores: `?`, `'texto'` (`''` para uma aspa), números, `true`, `false`. `and` liga mais forte que `or`, parênteses agrupam, palavras-chave em qualquer caixa. A expressão pode ser combinada com as outras chamadas (`where`, `or_where`, grupos com função). Um campo com ponto é um caminho em sub-documento (`endereco.uf`); não há apelidos de tabela (`A.CAMPO`) nem joins.

**Precedência:** numa cadeia que mistura `where` e `or_where`, vale a do SQL: `where(a):or_where(b):where(c)` é `a or (b and c)`. O plano entregue ao driver deixa isso explícito (cada trecho de `and` vira um grupo), então todo driver avalia igual, seja concatenando SQL ou avaliando da esquerda para a direita.

**nil:** `where("email", nil)` (ou `email = ?` com `nil`) entra no plano como `is_null`, e `where("email", "!=", nil)` como `is_not_null`.

```sea
-- Atualização parcial (suporta campos aninhados via dot-path)
local afetados = clientes:where("id", novo.id):update({
    saldo = 500.0,
    ["endereco.cidade"] = "Campinas",
})?

-- Remoção
local removidos = clientes:where("ativo", false):delete()?

-- Inserir ou atualizar pela chave primária (num comando só se o driver tem upsert)
local salvo = clientes:save({id = 7, nome = "Maria", email = "maria@example.com"})?
```

### Agregações e paginação

```sea
-- Uma linha por grupo, com os campos do group_by e cada agregado nomeado
local por_uf = clientes:where("ativo", true)
    :group_by("endereco.uf")
    :aggregate({n = "count(*)", total = "sum(saldo)", media = "avg(saldo)"})
    :order_by("-total")      -- order_by pode usar o nome de um agregado
    :limit(5)
    :all()?
-- por_uf[0] → {["endereco.uf"] = "SP", n = 3, total = 90.0, media = 30.0}

local total = clientes:sum("saldo")?                  -- também avg, min e max
local maior = clientes:where("endereco.uf", "SP"):max("saldo")?

local pagina = clientes:order_by("nome"):paginate(2, 20)?
-- {items = [...], total = 57, page = 2, per_page = 20, pages = 3}
```

As funções de agregação são:
- `count(*)`;
- `count(campo)`;
- `sum`, `avg`, `min` e `max` de um campo.

Elas seguem a regra do SQL: valores nil são ignorados, e `sum`/`avg`/`min`/`max` sem nenhum valor dão `nil`. Com agregados, `limit`, `offset` e `order_by` valem para os grupos, e `count()` conta os grupos.

Um driver com `conn:aggregate(schema, plan)` calcula tudo no banco; o `rdd.sql` implementa esse método com `GROUP BY`. Sem ele, o RDD busca os documentos com `find` e agrupa em memória.

`paginate(página, por_página)` começa a contar as páginas em 1. O campo `total` traz o número de documentos que satisfazem a query, sem a paginação.

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
        function conn:upsert(schema, doc) ... end          -- → doc_salvo | error (save() num comando só)
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

### Drivers SQL: `rdd.sql`

Para bancos SQL não é preciso traduzir o plano à mão. O `rdd.sql` (arquivo `sql.sea`) compila o plano em SQL parametrizado, e as diferenças entre bancos ficam num **dialeto**: `rdd.sql.dialects.sqlite`, `.postgres` e `.sqlserver`.

Um driver SQLite completo:

```sea
local sqlite = require("sqlite")

rdd.register_driver("sqlite", {
    open = function(opts)
        local db = sqlite.open(opts.path or ":memory:")?
        return rdd.sql.connection(db, rdd.sql.dialects.sqlite)
    end,
})
```

`sql.connection(db, dialeto)` implementa todo o contrato do driver. O `db` precisa ter:
- `query(sql, params)`, que devolve as linhas;
- `exec(sql, params)`, que devolve quantas linhas mudaram;
- `value(sql, params)`, que devolve a primeira coluna da primeira linha;
- `close()`;
- opcionalmente, `last_insert_id()` e `transaction(fn)`.

O handle da biblioteca `sqlite` do Sea já serve como está. Para um banco cuja API não bate, o driver pode usar as funções de compilação diretamente:

| Função | Resultado |
| --- | --- |
| `sql.find(schema, plan, d)` | `{sql, params}` do `SELECT` |
| `sql.aggregate(schema, plan, d)` | `{sql, params}` do `SELECT ... GROUP BY` de um plano com agregados |
| `sql.count(schema, plan, d)` | `{sql, params}` do `SELECT COUNT(*)`, respeitando `limit`/`offset` |
| `sql.insert(schema, doc, d)` | `{sql, params}` do `INSERT` (com `RETURNING` no postgres) |
| `sql.upsert(schema, doc, d)` | `{sql, params}` que insere ou atualiza pela chave primária (`ON CONFLICT` no SQLite/postgres, `MERGE` no SQL Server) |
| `sql.update(schema, plan, changes, d)` | `{sql, params}` do `UPDATE`; uma chave `"endereco.cidade"` altera só esse valor dentro da coluna JSON |
| `sql.delete(schema, plan, d)` | `{sql, params}` do `DELETE` |
| `sql.create_table(schema, d)` | o `CREATE TABLE` que não recria uma tabela existente |
| `sql.where(schema, filters, d, params)` | só a condição, acrescentando os valores em `params` (para SQL escrito à mão) |
| `sql.hydrate(schema, row, fields, d)` | a linha de volta como documento: bools e colunas JSON decodificados |

O que o compilador garante:
- Os valores vão sempre como parâmetros.
- Os nomes de tabela e coluna vão entre aspas do dialeto (`"x"` ou `[x]`).
- `contains`, `starts_with` e `ends_with` escapam `%` e `_` do valor.
- `in []` vira uma condição falsa.
- `= nil` vira `IS NULL`.
- `ilike` vira `LOWER(..) LIKE LOWER(..)` onde o banco não tem `ILIKE`.

Os campos aninhados são lidos com `json_extract` no SQLite, `#>>` no postgres e `JSON_VALUE` no SQL Server.

Para outro banco, ou para ajustar um dialeto existente, use `rdd.sql.dialect(sobrescritas [, base])`. Os campos que podem ser sobrescritos são:
- `placeholder(n)`;
- `quote(nome)`;
- `has_ilike`;
- `bind(v, campo)` e `read(v, campo)`;
- `json_get` e `json_set`;
- `paging`;
- `types` (o tipo de coluna de cada tipo do schema);
- `auto_pk`;
- `returning`;
- `create_table`.

Por exemplo:

```sea
local mysql = rdd.sql.dialect({quote = function(n) return "`" .. n .. "`" end})
```

---

## 4. Executando os Testes

```sh
~/projects/sea_vm/bin/sea check *.sea
~/projects/sea_vm/bin/sea test -v
```
