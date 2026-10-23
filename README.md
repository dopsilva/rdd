# RDD — Replaceable Database Driver para Sea

O **RDD** é uma camada de abstração de dados e ORM/ODM para a linguagem **Sea**, projetada sob o conceito de *Replaceable Database Driver*: você define suas estruturas de dados (planas ou aninhadas como documentos) e escreve suas queries de forma única, independente de onde os dados serão persistidos (PostgreSQL, SQLite, SQL Server, CockroachDB, ClickHouse, MongoDB, S3, etc.).

O RDD cuida de:
1. **Registro e validação de contrato de drivers** (`rdd.register_driver`).
2. **Definição e validação de Schemas** (`rdd.schema`), suportando tanto modelos tabulares quanto documentos encadeados (`document`, `array` de sub-documentos, `map`, etc.), valores padrão (`default_val`), campos gerados (`auto`) e validação de tipos.
3. **Query Builder Fluente** (`where`/`and`/`or` com operadores, `in`, `not_in`, `between`, `is_null`, expressões, grupos e `rdd.raw`, `fields`, `order_by`, `limit`, `offset`, `join`/`left_join`, `group_by`/`aggregate`, `paginate`), gerando um **Query Plan (AST)** abstrato que é repassado junto com o `Schema` e o documento bruto para o driver registrado.

---

## Instalação

Num projeto Sea, pelo `sea get` (o pacote vai para `sea_modules/` e a versão fica no `sea.lock`):

```sh
sea get https://github.com/dopsilva/rdd
```

```sea
local rdd = require("rdd")
local cockroach = require("rdd/drivers/cockroach")   -- usa a lib cockroach do Sea, pelo nome
```

As bibliotecas do Sea que os drivers usam (`cockroach`, `clickhouse`, `sqlite`) são carregadas pelo nome: um `sea` instalado acha em `lib/sea/`, e o `bin/sea` do checkout do sea_vm acha em `libs/`. Os exemplos abaixo rodam de dentro deste repositório, por isso usam `require("./rdd")`.

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

Opções de um campo: `required`, `primary_key`, `auto`, `default_val`, `min`/`max` (tamanho de string ou array, valor de número), `enum` (a lista de valores aceitos: `status = {type = "string", enum = ["aberto", "pago"]}`), `unique`, `validate(v, caminho)`.

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
    :or(function(sub)
        sub:where("endereco.uf", "RJ"):where("nome", "in", ["Ana", "Bia"])
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

Os operadores mais comuns têm métodos com o próprio nome, ligados com `and`:

```sea
clientes:where("ativo", true)
    :in("uf", ["SP", "RJ"])
    :not_in("id", [3, 4])
    :between("saldo", 400, 1500)
    :is_null("email")                          -- e is_not_null
    :or(function(g)                            -- or / and: as mesmas formas de where
        g:where("nome", "Ana"):in("uf", ["PR"])
    end)
    :order_by("-saldo", "nome")
```

`where`, `and` e `or` aceitam todas estas formas (`where` e `and` ligam com `and`; `or` liga com `or`):

```sea
q:where("nome", "Ana")                         -- igualdade
q:where("saldo", ">", 100)                     -- = != <> < <= > >= like ilike
q:where("uf", "in", ["SP", "RJ"])              -- in, not in, contains, starts with, ends with
q:where("saldo", "between", 400, 1500)         -- between leva dois valores
q:where("email", nil)                          -- is null (where("email", "!=", nil): is not null)
q:where({ativo = true, uf = ["SP", "RJ"]})     -- tabela de igualdades; um array vira in
q:where("uf in ? and saldo > ?", ufs, 100)     -- expressão
q:where(function(g) g:where("a", 1):or("b", 2) end)  -- grupo entre parênteses
q:where(rdd.raw("lower(nome) = ?", "ana"))     -- SQL cru (veja "SQL cru")
```

O operador aceita qualquer caixa e espaços (`"NOT IN"`, `"starts with"`).

`order_by` aceita várias chaves de uma vez, na ordem de prioridade:

```sea
q:order_by("-saldo", "nome")          -- "-campo" decrescente; "campo" ou "+campo" crescente
q:order_by("saldo desc, nome")        -- estilo SQL, numa string só
q:order_by(["-saldo", "nome"])        -- um array
q:order_by("nome", "desc")            -- um campo e a direção
```

Na expressão: `campo op valor` com `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `like`, `ilike`, `contains`, `starts_with`, `ends_with`; `campo [not] in ('a', 'b')` ou `campo [not] in ?` (um array); `campo between 1 and 9`; `campo is [not] null`. Valores: `?`, `'texto'` (`''` para uma aspa), números, `true`, `false`. `and` liga mais forte que `or`, parênteses agrupam, palavras-chave em qualquer caixa. A expressão pode ser combinada com as outras chamadas (`where`, `or`, grupos com função). Um campo com ponto é um caminho em sub-documento (`endereco.uf`) ou, depois de um `join`, um campo da tabela juntada pelo seu apelido (`p.valor`, veja abaixo).

**Precedência:** numa cadeia que mistura `where` e `or`, vale a do SQL: `where(a):or(b):where(c)` é `a or (b and c)`. O plano entregue ao driver deixa isso explícito (cada trecho de `and` vira um grupo), então todo driver avalia igual, seja concatenando SQL ou avaliando da esquerda para a direita.

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

### Joins

```sea
local Pedidos = rdd.schema("pedidos", {
    id = {type = "int", primary_key = true, auto = true},
    cliente_id = "int",
    produto = "string",
    valor = "number",
})

-- join(schema ou model, apelido, "condição"): os campos de pedidos viram p.campo
local grandes = clientes:join(Pedidos, "p", "p.cliente_id = id")
    :where("p.valor > ? and endereco.uf = ?", 500, "SP")
    :order_by("-p.valor")
    :all()?
-- grandes[0] → {id = 1, nome = "Ana", ..., p = {id = 2, produto = "Monitor", valor = 900.0, ...}}

-- left_join mantém quem não tem par (doc.p fica nil); combina com agregações
local vendas = clientes:left_join(Pedidos, "p", "p.cliente_id = id")
    :group_by("nome")
    :aggregate({pedidos = "count(p.id)", total = "sum(p.valor)"})
    :order_by("-total")
    :all()?

-- quem nunca comprou
local sem_pedido = clientes:left_join(Pedidos, "p", "p.cliente_id = id"):where("p.id", nil):all()?
```

**Como funciona:**
- **Condição:** igualdades entre campos, ligadas por `and` (`"p.cliente_id = id and p.loja = loja"`). Um campo sem apelido é da tabela da query.
- **Vários joins:** dá para encadear quantos quiser; cada `on` pode citar os apelidos anteriores.
- **Onde o apelido vale:** depois do `join`, ele pode ser usado em `where` (inclusive em expressões), `order_by`, `group_by`, `aggregate` e `fields`. Em `fields`, o apelido sozinho (`fields("nome", "p")`) traz o documento juntado inteiro.
- **Restrições:** o apelido não pode repetir o nome de um campo da query, e o `join` precisa vir antes das chamadas que usam o apelido. `update` e `delete` não aceitam joins.

**Quem executa:**
- Um driver que declara `conn.joins = true` recebe `plan.joins` (`[{kind = "inner"|"left", alias, collection, schema, on = [{left, right}]}]`) e faz o join no banco. É o caso do `rdd.sql`, que gera `INNER JOIN`/`LEFT JOIN` com todas as colunas qualificadas.
- Para os outros drivers, o RDD lê cada tabela com `find`, junta em memória e aplica filtros, ordem, paginação e agregações. Os filtros que tocam só uma tabela seguem para o `find` dela antes do join, para ler menos.

### SQL cru

Quando o construtor de queries não alcança (uma função do banco, um `WITH`, um relatório complexo), há três níveis:

```sea
-- 1. Um comando inteiro: linhas como o banco devolve, ou o número de linhas alteradas
local linhas = db:raw("SELECT uf, count(*) AS n FROM clientes GROUP BY uf HAVING count(*) > ?", 10)?
local n = db:exec("UPDATE clientes SET nome = upper(nome) WHERE id = ?", 7)?

-- 2. Pelo model: cada linha volta como documento do schema (bool, JSON decodificados)
local vips = clientes:raw("SELECT * FROM clientes WHERE saldo > (SELECT avg(saldo) FROM clientes)")?

-- 3. Um trecho cru dentro de uma query normal, entre parênteses
local ana = clientes:where("ativo", true):where(rdd.raw("lower(nome) = lower(?)", "ANA")):first()?
```

**Regras:**
- Os `?` viram o marcador de parâmetro do dialeto (`?`, `$1` ou `@p1`); `??` escreve um `?` literal, por exemplo para o operador `?` do jsonb.
- Um `?` dentro de aspas não é tocado.
- O texto cru não é validado pelo schema. Com joins, as colunas vão como o SQL as nomeia (`"p".valor`).
- Os três níveis precisam de um driver SQL: os dois primeiros usam os métodos `conn:raw_query` e `conn:raw_exec`, e a condição `rdd.raw` exige `conn.raw = true`. O `rdd.sql.connection` tem os três.

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

Para bancos SQL não é preciso traduzir o plano à mão. O `rdd.sql` (arquivo `sql.sea`) compila o plano em SQL parametrizado, e as diferenças entre bancos ficam num **dialeto**: `rdd.sql.dialects.sqlite`, `.postgres`, `.sqlserver`, `.cockroach` e `.clickhouse`.

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

Os campos aninhados são lidos com `json_extract` no SQLite, `#>>` no postgres e no CockroachDB, e `JSON_VALUE` no SQL Server e no ClickHouse. Um campo aninhado declarado como `int` ou `number` é convertido para número antes de comparar, ordenar ou agregar (onde o banco devolve o valor do JSON como texto, `"9" > "50"`).

Para outro banco, ou para ajustar um dialeto existente, use `rdd.sql.dialect(sobrescritas [, base])`. Os campos que podem ser sobrescritos são:
- `placeholder(n)`;
- `quote(nome)`;
- `has_ilike`;
- `bind(v, campo)` e `read(v, campo)`;
- `json_get`, `json_set` e `json_number` (lê como número o texto devolvido por `json_get`);
- `like_escape` (a cláusula `ESCAPE` depois dos `LIKE` de `contains`/`starts_with`/`ends_with`);
- `paging`;
- `types` (o tipo de coluna de cada tipo do schema);
- `auto_pk`;
- `returning`;
- `create_table`.

Por exemplo:

```sea
local mysql = rdd.sql.dialect({quote = function(n) return "`" .. n .. "`" end})
```

### Drivers prontos: CockroachDB e ClickHouse

A pasta `drivers/` traz drivers que usam as bibliotecas `libs/cockroach` e `libs/clickhouse` do sea_vm (escritas só em Sea):

```sea
rdd.register_driver("cockroach", require("./drivers/cockroach"))
local db = rdd.open("cockroach", {url = "postgresql://root@localhost:26257/defaultdb?sslmode=disable", max_conns = 8})?

rdd.register_driver("clickhouse", require("./drivers/clickhouse"))
local eventos = rdd.open("clickhouse", {url = "http://default:senha@localhost:8123/default"})?
```

Em ambos, `url` (ou `uri`) é o endereço, e as outras opções vão para o `open` da biblioteca (`max_conns`, `sslmode`, `sslrootcert`, `password`, `settings`, ...). `cockroach.connection(db)` e `clickhouse.connection(ch)` embrulham uma conexão já aberta.

**CockroachDB** (dialeto `cockroach`, que também serve para PostgreSQL com `{dialect = rdd.sql.dialects.postgres}`):
- Tudo roda no banco: filtros, joins, agregações, paginação, e o `save()` vira `INSERT ... ON CONFLICT`.
- As chaves `auto` são colunas `GENERATED BY DEFAULT AS IDENTITY` (1, 2, 3...). Sub-documentos, arrays e mapas são colunas `JSONB`.
- A conexão é um pool que várias goroutines usam ao mesmo tempo.
- `db:transaction(fn)` roda `fn` na conexão da transação. Quando o CockroachDB acusa conflito (SQLSTATE 40001), roda `fn` de novo numa transação nova, então `fn` deve mexer só no banco.

**ClickHouse** (dialeto `clickhouse`): é um banco analítico, e algumas coisas mudam.
- **Parâmetros:** os valores vão como parâmetros tipados do servidor (`{p1:Int64}`); o tipo sai do valor.
- **Tabelas:** o `migrate` cria tabelas `MergeTree` ordenadas pela chave primária (`engine` troca o motor). Campos que não são `required` são `Nullable`, e documentos, arrays e mapas são texto JSON em colunas `String`. A chave não é única: o motor não confere.
- **Chaves `auto`:** quem gera é o driver, com a maior chave mais 1, guardada por conexão. Por isso, só um processo deve inserir numa tabela com chave `auto`. Os `insert_many` vão num único lote.
- **`update` e `delete`:** viram mutações (`ALTER TABLE ... UPDATE`, `DELETE` leve) que o driver espera terminar. A chave não pode ser alterada. O `save()` apaga a linha com a chave e insere a nova.
- **Sem transações:** `db:transaction(fn)` só executa `fn`, e nada é desfeito se falhar.

---

## 4. Executando os Testes

```sh
~/projects/sea_vm/bin/sea check *.sea
~/projects/sea_vm/bin/sea test -v
```

`drivers_test.sea` roda a mesma suíte (`driver_suite.sea`: CRUD, operadores, campos aninhados, agregações, joins, SQL cru, `save` e transações) em todos os drivers. Ela sempre roda no SQLite; no CockroachDB e no ClickHouse, roda quando os servidores são informados. A suíte apaga e recria as tabelas `clientes` e `pedidos`.

```sh
COCKROACH_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
CLICKHOUSE_URL="http://default:senha@localhost:8123/default" \
~/projects/sea_vm/bin/sea test -v drivers_test.sea
```

## 5. Exemplos

```sh
~/projects/sea_vm/bin/sea examples/01_schemas_and_queries.sea
```

| Exemplo | O que mostra |
| --- | --- |
| `01_schemas_and_queries.sea` | Schemas aninhados e validação, sem banco. Também o plano gerado por consultas com expressão, `nil`, erros, join e agregação. |
| `02_custom_driver_memory.sea` | Um driver de documentos em memória que implementa só o obrigatório e usa `rdd.match`. Join, agregação, `count` e `save` funcionam pelo RDD sobre o `find` dele. |
| `03_sqlite_driver.sea` | Uma loja (clientes, produtos e pedidos) no SQLite pelo `rdd.sql`, com tudo rodando no banco. Mostra expressões, operadores, dois joins, vendas por categoria, `left join` para quem nunca comprou, upsert, paginação e transação com rollback. |
| `04_sql_dialects.sea` | O mesmo plano compilado para SQLite, PostgreSQL, SQL Server e um dialeto MySQL escrito no próprio exemplo. |
| `05_cockroach_driver.sea` | Uma loja no CockroachDB. Ids da coluna identity, filtro dentro do JSONB, oito compras simultâneas do último estoque (as transações em conflito se repetem e exatamente cinco passam), vendas com join, `save` e SQL cru. Usa `COCKROACH_URL`. |
| `06_clickhouse_driver.sea` | 20 mil eventos de um site num lote só no ClickHouse. Visitas por seção com join, conversão por país, filtros em sub-documento, paginação, quantis em SQL cru, e `update`/`delete` como mutações. Usa `CLICKHOUSE_URL`. |
