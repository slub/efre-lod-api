# Generate Test Data
* to generate a test data set out for Linked data, use the `genLDTestSet.py` script with the config of your lod-api, i.e.

  ```sh
  python3 genLDTestSet.py --config /etc/lod-apiconfig.yml [--outdir ldj]
  ```
  the json-LD files are generated in the output directory (outdir, default: "./ldj")

* reindex your elasticsearch by providing the containing folder of the formerly generated data as well as the host
  ```sh
  ./reloadLDTestSet.sh ldj localhost
  ```
