-- =====================================================
-- 5 CONSULTAS HIVEQL REPRESENTATIVAS
-- Dataset: E-commerce Analytics
-- =====================================================

USE ecommerce_analytics;

-- =====================================================
-- CONSULTA 1: Análisis de Ventas por Categoría y Período
-- Funciones: Agregaciones, Window Functions, Date Functions
-- =====================================================
SELECT 
    p.category,
    YEAR(o.order_date) as year,
    MONTH(o.order_date) as month,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.quantity) as total_items_sold,
    SUM(oi.total_price) as total_revenue,
    AVG(oi.total_price) as avg_item_price,
    -- Window function para calcular crecimiento mensual
    LAG(SUM(oi.total_price), 1) OVER (
        PARTITION BY p.category 
        ORDER BY YEAR(o.order_date), MONTH(o.order_date)
    ) as prev_month_revenue,
    -- Cálculo de porcentaje de crecimiento
    ROUND(
        ((SUM(oi.total_price) - LAG(SUM(oi.total_price), 1) OVER (
            PARTITION BY p.category 
            ORDER BY YEAR(o.order_date), MONTH(o.order_date)
        )) / LAG(SUM(oi.total_price), 1) OVER (
            PARTITION BY p.category 
            ORDER BY YEAR(o.order_date), MONTH(o.order_date)
        )) * 100, 2
    ) as growth_percentage
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'completed'
    AND o.order_date >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY p.category, YEAR(o.order_date), MONTH(o.order_date)
ORDER BY p.category, year DESC, month DESC;

-- =====================================================
-- CONSULTA 2: Top 10 Clientes con Mayor Valor (CLV)
-- Funciones: CTE, Ranking, Conditional Aggregation
-- =====================================================
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) as customer_name,
        c.customer_segment,
        c.country,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        DATEDIFF(MAX(o.order_date), MIN(o.order_date)) + 1 as customer_lifetime_days,
        -- Calcular frecuencia de compra
        ROUND(COUNT(DISTINCT o.order_id) / 
              ((DATEDIFF(MAX(o.order_date), MIN(o.order_date)) + 1) / 30.0), 2) as orders_per_month,
        -- Segmentación RFM simplificada
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) <= 30 THEN 'Active'
            WHEN DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) <= 90 THEN 'At Risk'
            ELSE 'Inactive'
        END as recency_status
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'completed'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment, c.country
),
customer_rankings AS (
    SELECT *,
        -- Ranking por valor total
        ROW_NUMBER() OVER (ORDER BY total_spent DESC) as value_rank,
        -- Ranking por frecuencia
        ROW_NUMBER() OVER (ORDER BY total_orders DESC) as frequency_rank,
        -- Score combinado
        (total_spent * 0.6) + (total_orders * avg_order_value * 0.4) as clv_score
    FROM customer_metrics
)
SELECT 
    value_rank,
    customer_name,
    customer_segment,
    country,
    total_orders,
    total_spent,
    avg_order_value,
    orders_per_month,
    customer_lifetime_days,
    recency_status,
    ROUND(clv_score, 2) as customer_lifetime_value
FROM customer_rankings
WHERE value_rank <= 10
ORDER BY value_rank;

-- =====================================================
-- CONSULTA 3: Análisis de Productos con Mejor Performance
-- Funciones: Multiple JOINs, Subqueries, Statistical Functions
-- =====================================================
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    -- Métricas de ventas
    COUNT(DISTINCT oi.order_id) as times_ordered,
    SUM(oi.quantity) as total_units_sold,
    SUM(oi.total_price) as total_revenue,
    ROUND(AVG(oi.quantity), 2) as avg_quantity_per_order,
    -- Métricas de reseñas
    COUNT(DISTINCT r.review_id) as total_reviews,
    ROUND(AVG(r.rating), 2) as avg_rating,
    SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) as positive_reviews,
    SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END) as negative_reviews,
    -- Rentabilidad
    ROUND((p.price - p.cost) / p.price * 100, 2) as profit_margin_pct,
    ROUND(SUM(oi.total_price) - (SUM(oi.quantity) * p.cost), 2) as total_profit,
    -- Rank por revenue
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.total_price) DESC) as revenue_rank_in_category
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id AND o.order_status = 'completed'
LEFT JOIN reviews r ON p.product_id = r.product_id
WHERE p.in_stock = true
GROUP BY p.product_id, p.product_name, p.category, p.brand, p.price, p.cost
HAVING COUNT(DISTINCT oi.order_id) >= 5  -- Solo productos con al menos 5 órdenes
ORDER BY total_revenue DESC
LIMIT 20;

-- =====================================================
-- CONSULTA 4: Análisis de Tendencias Estacionales
-- Funciones: CASE WHEN, Date Functions, Percentiles
-- =====================================================
SELECT 
    CASE 
        WHEN MONTH(o.order_date) IN (12, 1, 2) THEN 'Invierno'
        WHEN MONTH(o.order_date) IN (3, 4, 5) THEN 'Primavera'
        WHEN MONTH(o.order_date) IN (6, 7, 8) THEN 'Verano'
        WHEN MONTH(o.order_date) IN (9, 10, 11) THEN 'Otoño'
    END as season,
    MONTH(o.order_date) as month,
    MONTHNAME(o.order_date) as month_name,
    p.category,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.quantity) as total_items,
    SUM(oi.total_price) as total_revenue,
    ROUND(AVG(o.total_amount), 2) as avg_order_value,
    -- Análisis de distribución de órdenes
    PERCENTILE_APPROX(o.total_amount, 0.5) as median_order_value,
    PERCENTILE_APPROX(o.total_amount, 0.25) as q1_order_value,
    PERCENTILE_APPROX(o.total_amount, 0.75) as q3_order_value,
    -- Comparación con promedio anual
    ROUND(
        (SUM(oi.total_price) / 
         (SELECT SUM(oi2.total_price) / 12 
          FROM orders o2 
          JOIN order_items oi2 ON o2.order_id = oi2.order_id 
          JOIN products p2 ON oi2.product_id = p2.product_id 
          WHERE o2.order_status = 'completed' 
          AND p2.category = p.category
          AND YEAR(o2.order_date) = YEAR(o.order_date)
         ) - 1) * 100, 2
    ) as vs_monthly_avg_pct
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'completed'
    AND YEAR(o.order_date) >= YEAR(CURRENT_DATE()) - 1
GROUP BY 
    CASE 
        WHEN MONTH(o.order_date) IN (12, 1, 2) THEN 'Invierno'
        WHEN MONTH(o.order_date) IN (3, 4, 5) THEN 'Primavera'
        WHEN MONTH(o.order_date) IN (6, 7, 8) THEN 'Verano'
        WHEN MONTH(o.order_date) IN (9, 10, 11) THEN 'Otoño'
    END,
    MONTH(o.order_date),
    MONTHNAME(o.order_date),
    p.category,
    YEAR(o.order_date)
ORDER BY p.category, month;

-- =====================================================
-- CONSULTA 5: Análisis de Cohortes de Clientes
-- Funciones: Advanced Window Functions, Self-JOINs, Date Arithmetic
-- =====================================================
WITH customer_cohorts AS (
    SELECT 
        customer_id,
        MIN(order_date) as first_order_date,
        DATE_FORMAT(MIN(order_date), 'yyyy-MM') as cohort_month
    FROM orders
    WHERE order_status = 'completed'
    GROUP BY customer_id
),
customer_orders AS (
    SELECT 
        o.customer_id,
        o.order_date,
        c.cohort_month,
        c.first_order_date,
        -- Calcular el período desde la primera compra
        FLOOR(MONTHS_BETWEEN(o.order_date, c.first_order_date)) as period_number
    FROM orders o
    JOIN customer_cohorts c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'completed'
),
cohort_data AS (
    SELECT 
        cohort_month,
        period_number,
        COUNT(DISTINCT customer_id) as customers
    FROM customer_orders
    GROUP BY cohort_month, period_number
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT 
    cd.cohort_month,
    cs.cohort_size,
    cd.period_number,
    cd.customers,
    ROUND((cd.customers * 100.0 / cs.cohort_size), 2) as retention_rate,
    -- Comparar con período anterior
    LAG(cd.customers) OVER (
        PARTITION BY cd.cohort_month 
        ORDER BY cd.period_number
    ) as prev_period_customers,
    -- Calcular tasa de retención relativa
    CASE 
        WHEN LAG(cd.customers) OVER (
            PARTITION BY cd.cohort_month 
            ORDER BY cd.period_number
        ) > 0 THEN
        ROUND(
            (cd.customers * 100.0 / LAG(cd.customers) OVER (
                PARTITION BY cd.cohort_month 
                ORDER BY cd.period_number
            )), 2
        )
        ELSE NULL
    END as period_retention_rate
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
WHERE cd.period_number <= 12  -- Analizar primeros 12 meses
ORDER BY cd.cohort_month, cd.period_number;
