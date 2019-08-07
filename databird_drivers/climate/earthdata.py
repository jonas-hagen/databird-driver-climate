from databird import BaseDriver
from databird import ConfigurationError
from databird import utils

from pydap.client import open_url
from pydap.cas.urs import setup_session
import xarray as xr


def parse_slice(value):
    """
    Parses a `slice()` from string, like `start:step:stop`.
    """
    if value:
        parts = value.split(":")
        if len(parts) == 1:
            parts = [None, parts[0]]
        elif len(parts) == 3:
            parts = [parts[0], parts[2], parts[1]]
    else:
        parts = []
    return slice(*[int(p) if p else None for p in parts])


class GesDiscDriver(BaseDriver):
    """
    Retrieve data from the NASA's Goddard Earth Sciences Data
    and Information Services Center (GES DISC).

    Configuration options:
      - login: Earthdata user name
      - password: Earthdata password
      - url: The full url to retrieve the data from. (Rendered with context.)
      - variables: Set of variable names to fetch.
      - subset: a key value dict in the form "dim: start:step:stop".
                "time: 0:2:7" means select every second time
                value starting from 0 to 7.

    Example configuration:
    ```
    login: adalbert
    password: 345nsdfk24kll
    url: https://goldsmr5.gesdisc.eosdis.nasa.gov:443/opendap/MERRA2/M2I3NPASM.5.12.4/{time:%Y}/{time:%m}/MERRA2_400.inst3_3d_asm_Np.{time:%Y%m%d}.nc4
    variables: [U, T, V]
    subset:
      lat: 20:1:30
      lon: 10:1:20
      lev: 10:
    ```
    """

    @classmethod
    def check_config(cls, config):
        super().check_config(config)
        # Check if the config seems valid here
        if "url" not in config:
            raise ConfigurationError("url is required.")
        if "password" not in config or "login" not in config:
            raise ConfigurationError("Login and password are required.")

    def is_available(self, context):
        raise NotImplementedError("is_available for GES DISC")

    def retrieve_single(self, context, target, name):
        if name != "default":
            raise ValueError("Only 'default' target is supported for now.")
        url = self._config["url"].format(**context)
        session = setup_session(
            self._config["login"], self._config["password"], check_url=url
        )
        print("Connection established")
        store = xr.backends.PydapDataStore.open(url, session=session)
        ds = xr.open_dataset(store)
        print("Dataset indexed")
        subset = {var: parse_slice(s) for var, s in self._config["subset"].items()}
        print("subset: " + str(subset))
        ds = ds.isel(**subset)
        if self._config["variables"]:
            for var in ds.variables:
                if var not in ds.dims and var not in self._config["variables"]:
                    print("droppig var " + var)
                    ds = ds.drop(var)
        print("save to disk")
        ds.to_netcdf(target)
