def measure_performance(params)
  response_time = params[:makespan]
  throughput = params[:throughput]
  scale = params[:compress_ratio]
  accuracy_rate = 6.2*scale - 0.6
  accuracy_rate = 0 if accuracy_rate < 0
  accuracy_rate = 1 if accuracy_rate > 1
  performance = accuracy_rate * throughput / response_time
  return performance
end