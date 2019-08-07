[![Build Status](https://www.travis-ci.org/jonas-hagen/databird-driver-climate.svg?branch=master)](https://www.travis-ci.org/jonas-hagen/databird-driver-climate)

# databird-driver-climate

Data sources for climate and atmospheric research:

* `climate.EcmwfDriver`: Retrieve data from the European Centre for Medium-Range Weather Forecasts (ECMWF) via their API
* `climate.C3SDriver`: Retrieve data from the Copernicus Climate Change Service (C3S) via their API
* `climate.GesDiscDriver`: Retrieve data from the NASA EarthData GES DISC service.

See also: [databird](https://github.com/jonas-hagen/databird)

## Installation

```
pip install databird-driver-climate
```

## Example config

The following configuration file uses all the drivers that are provided by this package.

```yaml
general:
  root: /tmp/databird

profiles:
  c3s:
    driver: climate.C3SDriver
    configuration:
      key: 1234:aabbcc-ddeeff-1234abcdefg

  ecmwf:
    driver: climate.EcmwfDriver
    configuration:
      key: env:$ECMWF_API_KEY
      email: "someone@example.com"

  gesdisc:
    driver: climate.GesDiscDriver
    configuration:
      login: username_xxx
      password: password_xxx

repositories: 
  era5:
    description: ERA5 data
    profile: c3s
    period: 1 day
    delay: 177 days
    start: 2019-02-01
    queue: slow
    targets:
      grib2: "{time:%Y}/era5_europe_{time:%Y%m%d}_00.grib"
    configuration:
      name: reanalysis-era5-complete
      request:
        dataset: era5
        class: ea
        type: an
        stream: oper
        expver: 1
        levtype: ml
        levelist: 71
        param: 131
        area: 70/-130/30/-60
        grid: 2/2
        date: "{time:%Y-%m-%d}"
        time: "{time:%H}"

  ecmwf/oper:
    description: ECMWF operational
    profile: ecmwf
    period: 1 days
    delay: 3 days
    start: 2019-08-01
    queue: slow
    targets:
      grib2: "{time:%Y}/ecmwf_oper_{time:%Y%m%d}.grib"
    configuration:
      mars:
        class: OD
        type: AN
        stream: OPER
        expver: 1
        grid: 1.125/1.125
        area: 90/-180/80/100
        levtype: ML
        levelist: ALL
        param: T/U/V
        date: "{time:%Y-%m-%d}"
        time: 00/06/12/18
        use: INFREQUENT
        format: GRIB2

  merra:
    description: MERRA-2
    profile: gesdisc
    period: 1 days
    delay: 180 days
    start: 2019-02-01
    queue: slow
    targets:
      default: "{time:%Y}/merra_{time:%Y-%m-%d}.nc"
    configuration:
      url: https://goldsmr5.gesdisc.eosdis.nasa.gov:443/opendap/MERRA2/M2I3NPASM.5.12.4/{time:%Y}/{time:%m}/MERRA2_400.inst3_3d_asm_Np.{time:%Y%m%d}.nc4
      variables: [U, T, V]
      subset:
        lat: "20:1:30"
        lon: "10:1:20"
        lev: "10:"

 
```


