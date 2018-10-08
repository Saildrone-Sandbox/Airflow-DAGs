from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class WeatherFileSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, forecast_hour, forecast_type, *args, **kwargs):
        super(WeatherFileSensor, self).__init__(*args, **kwargs)
        self.forecast_hour = forecast_hour
        self.forecast_type = forecast_type

    def poke(self, context):
        print context.get('task_instance').execution_date
        return True
