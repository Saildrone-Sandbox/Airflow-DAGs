from airflow.plugins_manager import AirflowPlugin
from weathermaker.sensors.weather_file_sensor import WeatherFileSensor


class WeathermakerPlugin(AirflowPlugin):
    name = 'weathermaker_plugin'
    operators = [WeatherFileSensor]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
