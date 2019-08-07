from databird import BaseDriver
from databird import ConfigurationError
from databird import utils
from ecmwfapi import ECMWFService


class EcmwfDriver(BaseDriver):
    """
    Retrieve data from the European Centre for Medium-Range Weather Forecasts (ECMWF).

    Configuration options:
      - mars: A dict containing the mars specifiers. (Rendered with context.)

    Example configuration:
    ```
    key: 4de24c37d557fef8ddc092e63d9419a6
    email: someone@example.com
    mars:
      dataset: 'tigge',
      step: '24',
      number: 'all',
      levtype: 'sl',
      date: '{time:%Y%m%d}',
      time: '{time:%H}',
      origin: 'all',
      type: 'pf',
      param: 'tp',
      area: '70/-130/30/-60',
      grid: '2/2',
    ```
    """

    @classmethod
    def check_config(cls, config):
        super().check_config(config)
        # Check if the config seems valid here
        if "mars" not in config:
            raise ConfigurationError("no mars request parameters provided.")
        if "target" in config["mars"]:
            raise ConfigurationError("'target' in mars parameters is not needed.")
        if "key" not in config or "email" not in config:
            raise ConfigurationError("Key and email are required.")

    def is_available(self, context):
        raise NotImplementedError("is_available for ECMWF")

    def retrieve_single(self, context, target, name):
        if name not in ["grib2", "grib"]:
            raise ValueError("Only 'grib2' target is supported for now.")
        request = utils.render_dict(self._config["mars"], context)
        server = ECMWFService("mars")
        server.execute(request, target)
