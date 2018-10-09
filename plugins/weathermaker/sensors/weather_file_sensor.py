import os

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


NAM_BASE_DIR = '/data/weatherdata/nam'


class WeatherFileSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, forecast_hour, forecast_type, *args, **kwargs):
        super(WeatherFileSensor, self).__init__(*args, **kwargs)
        self.forecast_hour = int(forecast_hour)
        self.forecast_type = forecast_type

    def poke(self, context):

        year = '2018'
        month = '09'
        date = '10'
        hour = '06'

        file_dir = os.path.join(NAM_BASE_DIR, year, month, date, hour, 'native')

        execution_date = context.get('task_instance').execution_date

        file_name = '{f_type}.t{hour}z.awip32.0p25.f{f_hour:03d}.{year}.{month}.{date}'
        file_name = file_name.format(f_type=self.forecast_type,
                                     hour=hour,
                                     f_hour=self.forecast_hour,
                                     year=year,
                                     month=month,
                                     date=date)

        file_path = os.path.join(file_dir, file_name)

        print('execution_date hour: ' + str(execution_date.hour))
        print('looking for file_path: ' + file_path)

        return os.path.exists(file_path)
