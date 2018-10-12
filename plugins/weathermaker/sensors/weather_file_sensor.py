import os

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


NAM_BASE_DIR = '/data/weatherdata/nam'


class WeatherFileSensor(BaseSensorOperator):
    """
    This checks for the existence of source weather files.  It continually
    runs poke() at the poke_interval set in the DAG until it returns True,
    at which point the dependent tasks can continue processing.
    """

    template_fields = ('file_path', )

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        super(WeatherFileSensor, self).__init__(*args, **kwargs)
        self.file_path = file_path

    def poke(self, context):

        # task_instance = context['task_instance']

        # year = str(task_instance.execution_date.year)
        # # Hardcoded-here for testing
        # month = '09'
        # date = '10'
        # hour = '06'

        # file_dir = os.path.join(NAM_BASE_DIR, year, month, date, hour, 'native')

        # file_name = '{f_type}.t{hour}z.awip32.0p25.f{f_hour:03d}.{year}.{month}.{date}'
        # file_name = file_name.format(f_type=self.forecast_type,
        #                              hour=hour,
        #                              f_hour=self.forecast_hour,
        #                              year=year,
        #                              month=month,
        #                              date=date)

        # file_path = os.path.join(file_dir, file_name)

        # task_instance.xcom_push('file_path', file_path)

        print('Looking for file_path: ' + self.file_path)

        return os.path.exists(self.file_path)
