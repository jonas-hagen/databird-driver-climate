from databird import BaseDriver
from databird import ConfigurationError
from databird import utils
import cdsapi


class ClimateDriver(BaseDriver):
    """
    Databird driver for the Copernicus Climate Change Service (C3S)

    Configuration options:
      - name: The C3S dataset name.
      - request: C3S request details as dict.

    Example configuration (key is optional and can be provided by `~/.cdsapirc`):
    ```
    key: 1234:3c417487-510f-4972-9e86-95dab485d607
    name: reanalysis-era5-complete
    request:
      dataset: era5,
      class: ea,
      type: an,
      stream: oper,
      expver: 1,
      levtype: ml,
      levelist: 71,
      param: 131,
      area: 70/-130/30/-60,
      grid: 2/2,
      date: "{time:%Y-%m-%d}",
      time: "{time:%H}",
    ```
    """

    @classmethod
    def check_config(cls, config):
        super().check_config(config)
        if "name" not in config or "request" not in config:
            raise ConfigurationError("name and request are missing")
        if not isinstance(config["request"], dict):
            raise ConfigurationError("request must be of type dict")

    def is_available(self, context):
        return True

    def retrieve_single(self, context, target, name):
        if name not in ["grib2", "grib"]:
            raise ValueError("Only 'grib2' target is supported for now.")
        if "key" in self._config:
            params = dict(
                key=self._config["key"], url="https://cds.climate.copernicus.eu/api/v2"
            )
        else:
            params = dict()
        request = utils.render_dict(self._config["request"], context)
        cds_name = self._config["name"]
        client = cdsapi.Client(**params)
        client.retrieve(cds_name, request, target)
