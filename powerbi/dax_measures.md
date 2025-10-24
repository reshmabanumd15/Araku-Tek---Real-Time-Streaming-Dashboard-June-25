Orders = SUM(orders_hourly_final[events])
Revenue = SUM(orders_hourly_final[amount_sum])
Revenue per Order = DIVIDE([Revenue],[Orders])
Orders Last 60min = CALCULATE([Orders], DATESINPERIOD('Calendar'[DateTime], MAX('Calendar'[DateTime]), -60, MINUTE))
