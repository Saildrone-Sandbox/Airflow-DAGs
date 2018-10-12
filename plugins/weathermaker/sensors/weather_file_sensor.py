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

        print('Looking for file_path: ' + self.file_path)

        return os.path.exists(self.file_path)
