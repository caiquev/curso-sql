WITH tb_transacoes AS 
(
    SELECT IdTransacao,
    idCliente,
    QtdePontos,
    datetime(substr(DtCriacao,1,19)) AS DtCriacao,
    julianday('{date}') - julianday(substr(DtCriacao,1,10)) AS diffDate,
    CAST(strftime('%H', substr(DtCriacao, 1, 19)) AS INTEGER) AS dtHora
    FROM transacoes
    WHERE DtCriacao < '{date}'
),

tb_cliente AS 
(
    SELECT IdCliente,
    datetime(substr(DtCriacao,1,19)) AS DtCriacao,
    julianday('{date}') - julianday(substr(DtCriacao,1,10)) AS IdadeCliente
    FROM clientes
),

tb_sumario_transacoes AS (

SELECT IdCliente,
    count(CASE WHEN diffDate <= 7 THEN IdTransacao END) AS QtTransacoes7,
    count(CASE WHEN diffDate <= 14 THEN IdTransacao END) AS QtTransacoes14,
    count(CASE WHEN diffDate <= 28 THEN IdTransacao END) AS QtTransacoes28,
    count(CASE WHEN diffDate <= 56 THEN IdTransacao END) AS QtTransacoes56,

    count(IdTransacao) AS QtTransacoesVida,

    min(diffDate) AS DiasUltimaInteracao,

    sum(QtdePontos) AS SaldoPontos,

    sum(CASE WHEN QtdePontos > 0 THEN QtdePontos ELSE 0 END) AS QtdePontosPosVida,

    sum( CASE WHEN  QtdePontos > 0 AND diffDate <= 56 THEN QtdePontos ELSE 0 END) AS QtdePontosPos56,
    sum( CASE WHEN  QtdePontos > 0 AND diffDate <= 28 THEN QtdePontos ELSE 0 END) AS QtdePontosPos28,
    sum( CASE WHEN  QtdePontos > 0 AND diffDate <= 14 THEN QtdePontos ELSE 0 END) AS QtdePontosPos14,
    sum( CASE WHEN  QtdePontos > 0 AND diffDate <=  7 THEN QtdePontos ELSE 0 END) AS QtdePontosPos7,


    sum(CASE WHEN QtdePontos < 0 THEN QtdePontos ELSE 0 END) AS QtdePontosNegVida,
    sum( CASE WHEN  QtdePontos < 0 AND diffDate <= 56 THEN QtdePontos ELSE 0 END) AS QtdePontosNeg56,
    sum( CASE WHEN  QtdePontos < 0 AND diffDate <= 28 THEN QtdePontos ELSE 0 END) AS QtdePontosNeg28,
    sum( CASE WHEN  QtdePontos < 0 AND diffDate <= 14 THEN QtdePontos ELSE 0 END) AS QtdePontosNeg14,
    sum( CASE WHEN  QtdePontos < 0 AND diffDate <=  7 THEN QtdePontos ELSE 0 END) AS QtdePontosNeg7
    

FROM tb_transacoes

GROUP BY IdCliente
),

tb_transacao_produto AS (

    SELECT t1.*,
        t3.DescNomeProduto,
        t3.DescCategoriaProduto


    FROM tb_transacoes as t1

    LEFT JOIN transacao_produto AS t2
    ON t1.IdTransacao = t2.IdTransacao

    LEFT JOIN produtos AS t3
    ON t2.IdProduto = t3.IdProduto

),

tb_cliente_produto AS 
(
    SELECT IdCliente,
        DescNomeProduto,
        count(*) AS QtDVida,
        count( CASE WHEN diffDate <= 56 THEN IdTransacao END) AS QtD56,
        count( CASE WHEN diffDate <= 28 THEN IdTransacao END) AS QtD28,
        count( CASE WHEN diffDate <= 14 THEN IdTransacao END) AS QtD14,
        count( CASE WHEN diffDate <= 7 THEN IdTransacao END) AS QtD7

    FROM tb_transacao_produto
    GROUP BY IdCliente,DescNomeProduto
),
tb_cliente_produto_rn AS
(
    SELECT *,
        row_number() OVER (PARTITION BY IdCliente ORDER BY QtDVida DESC) AS rnVida,
        row_number() OVER (PARTITION BY IdCliente ORDER BY QtD56 DESC) AS rn56,
        row_number() OVER (PARTITION BY IdCliente ORDER BY QtD28 DESC) AS rn28,
        row_number() OVER (PARTITION BY IdCliente ORDER BY QtD14 DESC) AS rn14,
        row_number() OVER (PARTITION BY IdCliente ORDER BY QtD7 DESC) AS rn7

    FROM tb_cliente_produto
),

tb_cliente_dia AS  

(
    SELECT IdCliente,
        strftime('%w',DtCriacao) AS DtDia,
        count(*) AS QtTransacao
    FROM tb_transacoes
    WHERE diffDate <= 28
    GROUP BY IdCliente, DtDia
),

tb_cliente_dia_rn AS 
(
    SELECT *,
        row_number() OVER (PARTITION BY IdCliente  ORDER BY QtTransacao DESC) AS rnDia
    
    FROM tb_cliente_dia
),

tb_cliente_periodo AS (
    SELECT IdCliente,
            CASE 
                WHEN dtHora BETWEEN 7 AND 12 THEN 'Manhã'
                WHEN dtHora BETWEEN 13 AND 18 THEN 'Tarde'
                WHEN dtHora BETWEEN 19 AND 23 THEN 'Noite'
                ELSE 'Madrugada'
            END AS periodo,
            count(*) AS QtdeTransacao

    FROM tb_transacoes
    WHERE diffDate <= 28

    GROUP BY 1,2
),

tb_cliente_periodo_rn AS (
    SELECT *,
            ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY QtdeTransacao DESC) AS rnPeriodo
            FROM tb_cliente_periodo
),

tb_join AS (
    SELECT t1.*,
    t2.IdadeCliente,
    t3.DescNomeProduto AS ProdutoVida,
    t4.DescNomeProduto AS ProdutoD56,
    t5.DescNomeProduto AS ProdutoD28,
    t6.DescNomeProduto AS ProdutoD14,
    t7.DescNomeProduto AS ProdutoD7,
    COALESCE(t8.DtDia, -1) AS DtDia,
    COALESCE(t9.Periodo,'SEM INFORMACAO') AS PeriodoMaisTransacao28

    FROM tb_sumario_transacoes as t1
    LEFT JOIN tb_cliente AS t2
    ON t1.IdCliente = t2.IdCliente

    LEFT JOIN tb_cliente_produto_rn AS t3
    ON t1.IdCliente = t3.IdCliente
    AND t3.rnVida = 1

    LEFT JOIN tb_cliente_produto_rn AS t4
    ON t1.IdCliente = t4.IdCliente
    AND t4.rn56 = 1

    LEFT JOIN tb_cliente_produto_rn AS t5
    ON t1.IdCliente = t5.IdCliente
    AND t5.rn28 = 1

    LEFT JOIN tb_cliente_produto_rn AS t6
    ON t1.IdCliente = t6.IdCliente
    AND t6.rn14 = 1

    LEFT JOIN tb_cliente_produto_rn AS t7
    ON t1.IdCliente = t7.IdCliente
    AND t7.rn7 = 1

    LEFT JOIN tb_cliente_dia_rn AS t8
    ON t1.idCliente = t8.idCliente
    AND t8.rnDia = 1

    LEFT JOIN tb_cliente_periodo_rn AS t9
    ON t1.IdCliente = t9.IdCliente
    AND t9.rnPeriodo = 1

)


SELECT '{date}' as dtRef,
    *,
    1. * QtTransacoes28 / QtTransacoesVida AS engajamento28Vida

FROM tb_join
ORDER BY IdCliente



