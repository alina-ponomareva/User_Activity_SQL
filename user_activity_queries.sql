  -- 1. Collecting data on accounts

WITH
  account_metrics AS(
  SELECT
    s.date AS date,
    sp.country AS country,
    a.send_interval AS send_interval,
    a.is_verified AS is_verified,
    a.is_unsubscribed AS is_unsubscribed,
    COUNT(DISTINCT a.id) AS account_cnt
  FROM
    `DA.account` a
  JOIN
    `DA.account_session` acs
  ON
    a.id = acs.account_id
  JOIN
    `DA.session` s
  ON
    acs.ga_session_id = s.ga_session_id
  JOIN
    `DA.session_params` sp
  ON
    s.ga_session_id = sp.ga_session_id
  GROUP BY
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed ),
  
  -- 2. Calculating the metrics on account_cnt letters
  
email_metrics AS(
SELECT
  DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
  sp.country AS country,
  a.send_interval AS send_interval,
  a.is_verified AS is_verified,
  a.is_unsubscribed AS is_unsubscribed,
  COUNT(DISTINCT es.id_message) AS sent_msg,
  COUNT(DISTINCT eo.id_message) AS open_msg,
  COUNT(DISTINCT ev.id_message) AS visit_msg
FROM
  `DA.email_sent` es
LEFT JOIN
  `DA.email_open` eo
ON
  es.id_message = eo.id_message
LEFT JOIN
  `DA.email_visit` ev
ON
  es.id_message = ev.id_message
JOIN
  `DA.account` a
ON
  es.id_account = a.id
JOIN
  `DA.account_session` acs
ON
  a.id = acs.account_id
JOIN
  `DA.session` s
ON
  acs.ga_session_id = s.ga_session_id
JOIN
  `DA.session_params` sp
ON
  acs.ga_session_id = sp.ga_session_id
GROUP BY
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed ),
  
  --3. Using Union to combine data across accounts and emails
 
union_data AS(
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM
    account_metrics
  UNION ALL
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    0 AS account_cnt,
    sent_msg,
    open_msg,
    visit_msg
  FROM
    email_metrics ),
  
  --4. Grouping data after Union to avoid duplicates
  
  total_metrics AS(
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg
  FROM
    union_data
  GROUP BY
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed ),
  
  -- 5. Counting totals total_country_account_cnt — the total number of subscribers created by country and total_country_sent_cnt — the total number of emails sent by country
  
  total_country AS(
SELECT
  *,
  SUM(account_cnt) OVER(PARTITION BY country) AS total_country_account_cnt,
  SUM(sent_msg) OVER(PARTITION BY country) AS total_country_sent_cnt
FROM
  total_metrics ),
  
  --6. Counting the numbers rank_total_country_account_cnt — ranking of countries by the number of subscribers created by country and rank_total_country_sent_cnt — ranking of countries by the number of emails sent by country.
  
  rank_country AS(
SELECT
  *,
  DENSE_RANK() OVER(ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
  DENSE_RANK() OVER(ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
FROM
  total_country ) 
  
  --7. Final selection
  
SELECT
  *
FROM
  rank_country
WHERE
  rank_total_country_account_cnt <= 10
  OR rank_total_country_sent_cnt <= 10
ORDER BY
  date,
  country;
