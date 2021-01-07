# (c) 2012-2016 Continuum Analytics, Inc. / http://continuum.io
# All Rights Reserved
#
# conda is distributed under the terms of the BSD 3-clause license.
# Consult LICENSE.txt or http://opensource.org/licenses/BSD-3-Clause.
'''
We use the following conventions in this module:

    dist:        canonical package name, e.g. 'numpy-1.6.2-py26_0'

    ROOT_PREFIX: the prefix to the root environment, e.g. /opt/anaconda

    PKGS_DIR:    the "package cache directory", e.g. '/opt/anaconda/pkgs'
                 this is always equal to ROOT_PREFIX/pkgs

    prefix:      the prefix of a particular environment, which may also
                 be the root environment

Also, this module is directly invoked by the (self extracting) tarball
installer to create the initial environment, therefore it needs to be
standalone, i.e. not import any other parts of `conda` (only depend on
the standard library).
'''
import os
import re
import sys
import json
import shutil
import stat
from os.path import abspath, dirname, exists, isdir, isfile, islink, join
from optparse import OptionParser


on_win = bool(sys.platform == 'win32')
try:
    FORCE = bool(int(os.getenv('FORCE', 0)))
except ValueError:
    FORCE = False

LINK_HARD = 1
LINK_SOFT = 2  # never used during the install process
LINK_COPY = 3
link_name_map = {
    LINK_HARD: 'hard-link',
    LINK_SOFT: 'soft-link',
    LINK_COPY: 'copy',
}
SPECIAL_ASCII = '$!&\%^|{}[]<>~`"\':;?@*#'

# these may be changed in main()
ROOT_PREFIX = sys.prefix
PKGS_DIR = join(ROOT_PREFIX, 'pkgs')
SKIP_SCRIPTS = False
IDISTS = {
  "_ipyw_jlab_nb_ext_conf-0.1.0-py36h2fc01ae_0": {
    "md5": "30e0ffbedb4b7846d84316f1d4140127",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/_ipyw_jlab_nb_ext_conf-0.1.0-py36h2fc01ae_0.tar.bz2"
  },
  "alabaster-0.7.10-py36h174008c_0": {
    "md5": "53235aed2fd5873c19170a61266f620e",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/alabaster-0.7.10-py36h174008c_0.tar.bz2"
  },
  "anaconda-5.0.1-py36h6e48e2d_1": {
    "md5": "d2cee95d49d369ea20959abe434de154",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/anaconda-5.0.1-py36h6e48e2d_1.tar.bz2"
  },
  "anaconda-client-1.6.5-py36h04cfe59_0": {
    "md5": "0c4d9553ee0ba86c33bac67c2bf8021c",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/anaconda-client-1.6.5-py36h04cfe59_0.tar.bz2"
  },
  "anaconda-navigator-1.6.9-py36ha31b149_0": {
    "md5": "72336305c78f84ae4da57e00ba6d8eb3",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/anaconda-navigator-1.6.9-py36ha31b149_0.tar.bz2"
  },
  "anaconda-project-0.8.0-py36h99320b2_0": {
    "md5": "6e5bb1a8b29a64f9d3d8edb93b7f1a07",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/anaconda-project-0.8.0-py36h99320b2_0.tar.bz2"
  },
  "appnope-0.1.0-py36hf537a9a_0": {
    "md5": "2c2817783c67a92a00580d6ddf624567",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/appnope-0.1.0-py36hf537a9a_0.tar.bz2"
  },
  "appscript-1.0.1-py36h9e71e49_1": {
    "md5": "3971f46062666475eb8238072c479c77",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/appscript-1.0.1-py36h9e71e49_1.tar.bz2"
  },
  "asn1crypto-0.22.0-py36hb705621_1": {
    "md5": "6a136f351e5f9047e35c48790509ef03",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/asn1crypto-0.22.0-py36hb705621_1.tar.bz2"
  },
  "astroid-1.5.3-py36h1333018_0": {
    "md5": "e89d11c0e7a3e6ddbd1572e4cbacf6ca",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/astroid-1.5.3-py36h1333018_0.tar.bz2"
  },
  "astropy-2.0.2-py36hf79c81d_4": {
    "md5": "59b0d8065f43fa3a47a19f2887b6aba6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/astropy-2.0.2-py36hf79c81d_4.tar.bz2"
  },
  "babel-2.5.0-py36h9f161ff_0": {
    "md5": "48e0666727228d7674a00abdb4fd7e14",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/babel-2.5.0-py36h9f161ff_0.tar.bz2"
  },
  "backports-1.0-py36ha3c1827_1": {
    "md5": "256eb3e9e61944d2940e38d1c31df11f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/backports-1.0-py36ha3c1827_1.tar.bz2"
  },
  "backports.shutil_get_terminal_size-1.0.0-py36hd7a2ee4_2": {
    "md5": "02641f7885240306f45f919bdcfe00c6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/backports.shutil_get_terminal_size-1.0.0-py36hd7a2ee4_2.tar.bz2"
  },
  "beautifulsoup4-4.6.0-py36h72d3c9f_1": {
    "md5": "9d4345fb5481b351dfae510d3afb0c66",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/beautifulsoup4-4.6.0-py36h72d3c9f_1.tar.bz2"
  },
  "bitarray-0.8.1-py36h20fa61d_0": {
    "md5": "8a8e3b09163a953f060e91034aa9e779",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/bitarray-0.8.1-py36h20fa61d_0.tar.bz2"
  },
  "bkcharts-0.2-py36h073222e_0": {
    "md5": "f61dcd9b3e0b8dcc1401c862c7181dff",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/bkcharts-0.2-py36h073222e_0.tar.bz2"
  },
  "blaze-0.11.3-py36h02e7a37_0": {
    "md5": "59ee3b410c5477993c0d85b6e8fb3fbc",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/blaze-0.11.3-py36h02e7a37_0.tar.bz2"
  },
  "bleach-2.0.0-py36h8fcea71_0": {
    "md5": "b6c2556c9bebd95d17e4c5dc6af5dd2e",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/bleach-2.0.0-py36h8fcea71_0.tar.bz2"
  },
  "bokeh-0.12.10-py36hfd5be35_0": {
    "md5": "f7986b8a153e669e8e47f19d7cd0a92f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/bokeh-0.12.10-py36hfd5be35_0.tar.bz2"
  },
  "boto-2.48.0-py36hdbc59ac_1": {
    "md5": "0cad049daf788fd5254f1e3592e91023",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/boto-2.48.0-py36hdbc59ac_1.tar.bz2"
  },
  "bottleneck-1.2.1-py36hbd380ad_0": {
    "md5": "833940cee15a3f9ba92259c2a7fcd40b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/bottleneck-1.2.1-py36hbd380ad_0.tar.bz2"
  },
  "bzip2-1.0.6-h92991f9_1": {
    "md5": "1ff6a3515b890a5a6b7d38c278640694",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/bzip2-1.0.6-h92991f9_1.tar.bz2"
  },
  "ca-certificates-2017.08.26-ha1e5d58_0": {
    "md5": "a2c71bb4e657dfa888c524e54bf859c0",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ca-certificates-2017.08.26-ha1e5d58_0.tar.bz2"
  },
  "certifi-2017.7.27.1-py36hd973bb6_0": {
    "md5": "29874daee5dfc8bc30662531f664aeea",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/certifi-2017.7.27.1-py36hd973bb6_0.tar.bz2"
  },
  "cffi-1.10.0-py36h880867e_1": {
    "md5": "e062248e059cd590559e2fb89aeaee39",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/cffi-1.10.0-py36h880867e_1.tar.bz2"
  },
  "chardet-3.0.4-py36h96c241c_1": {
    "md5": "baa4d3ffed6ba759da3eab42838a5991",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/chardet-3.0.4-py36h96c241c_1.tar.bz2"
  },
  "click-6.7-py36hec950be_0": {
    "md5": "0c563dd7ca84dbc738930da55c359e4f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/click-6.7-py36hec950be_0.tar.bz2"
  },
  "cloudpickle-0.4.0-py36h13b7e56_0": {
    "md5": "f27d6d40849e055368410f7ea89380ac",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/cloudpickle-0.4.0-py36h13b7e56_0.tar.bz2"
  },
  "clyent-1.2.2-py36hae3ad88_0": {
    "md5": "6c9e9a3747132a8de93cbe29472e99ee",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/clyent-1.2.2-py36hae3ad88_0.tar.bz2"
  },
  "colorama-0.3.9-py36hd29a30c_0": {
    "md5": "a066e3a6d3c8ab246f84ef5cb4458e90",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/colorama-0.3.9-py36hd29a30c_0.tar.bz2"
  },
  "conda-4.3.30-py36h173c244_0": {
    "md5": "bfe099f23c61836e5abfa30908ae00fa",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/conda-4.3.30-py36h173c244_0.tar.bz2"
  },
  "conda-build-3.0.27-py36hb78d8cd_0": {
    "md5": "c6bbf36b481682d92d4954d94a21a764",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/conda-build-3.0.27-py36hb78d8cd_0.tar.bz2"
  },
  "conda-env-2.6.0-h36134e3_0": {
    "md5": "0876f412b6123634607ae9c22a7bc805",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/conda-env-2.6.0-h36134e3_0.tar.bz2"
  },
  "conda-verify-2.0.0-py36he837df3_0": {
    "md5": "2cd453b5a7d2ee7c40567f863343ea18",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/conda-verify-2.0.0-py36he837df3_0.tar.bz2"
  },
  "contextlib2-0.5.5-py36hd66e5e7_0": {
    "md5": "619e1c516deaad3a27a5ae688c8bdcb1",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/contextlib2-0.5.5-py36hd66e5e7_0.tar.bz2"
  },
  "cryptography-2.0.3-py36h22d4226_1": {
    "md5": "35a0508aeb308fdddb1ffa9fe02b2e90",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/cryptography-2.0.3-py36h22d4226_1.tar.bz2"
  },
  "curl-7.55.1-h7601780_3": {
    "md5": "b9dcb38d92be05c2fbe5194d1acf7f39",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/curl-7.55.1-h7601780_3.tar.bz2"
  },
  "cycler-0.10.0-py36hfc81398_0": {
    "md5": "01738824dbe2b2a1aee464282d5d3a1c",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/cycler-0.10.0-py36hfc81398_0.tar.bz2"
  },
  "cython-0.26.1-py36hd51f8eb_0": {
    "md5": "691fb6958a4de38763c5cd67365db257",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/cython-0.26.1-py36hd51f8eb_0.tar.bz2"
  },
  "cytoolz-0.8.2-py36h290905f_0": {
    "md5": "be0d2bf764ade321355871a187c12e27",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/cytoolz-0.8.2-py36h290905f_0.tar.bz2"
  },
  "dask-0.15.3-py36hc3ad2d6_0": {
    "md5": "5dea19124d2c9a6a5c42192d4aafd82b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/dask-0.15.3-py36hc3ad2d6_0.tar.bz2"
  },
  "dask-core-0.15.3-py36hc0be6b7_0": {
    "md5": "32278b7777fd03bf1c8ec35b4bf15a5c",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/dask-core-0.15.3-py36hc0be6b7_0.tar.bz2"
  },
  "datashape-0.5.4-py36hfb22df8_0": {
    "md5": "6bbda11f22d2b3cea7f9d98ace4e821b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/datashape-0.5.4-py36hfb22df8_0.tar.bz2"
  },
  "dbus-1.10.22-h50d9ad6_0": {
    "md5": "0385e214e2b84ad25d999aeb454930f4",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/dbus-1.10.22-h50d9ad6_0.tar.bz2"
  },
  "decorator-4.1.2-py36h69a1b52_0": {
    "md5": "85654ed8ff457f823f0da752e649dd98",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/decorator-4.1.2-py36h69a1b52_0.tar.bz2"
  },
  "distributed-1.19.1-py36h4ae75d2_0": {
    "md5": "98dd348c5714add8a0cf64809ceb0905",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/distributed-1.19.1-py36h4ae75d2_0.tar.bz2"
  },
  "docutils-0.14-py36hbfde631_0": {
    "md5": "7fc0c54449e11ccb9abdafddfaa91a71",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/docutils-0.14-py36hbfde631_0.tar.bz2"
  },
  "entrypoints-0.2.3-py36hd81d71f_2": {
    "md5": "7a0a0b773880cd883ccee0fd799aad74",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/entrypoints-0.2.3-py36hd81d71f_2.tar.bz2"
  },
  "et_xmlfile-1.0.1-py36h1315bdc_0": {
    "md5": "15d28fd6545a403784bddc62b2bcaf9f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/et_xmlfile-1.0.1-py36h1315bdc_0.tar.bz2"
  },
  "expat-2.2.4-h8f26bf8_1": {
    "md5": "276cc25a2dce382f6af72e990147e050",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/expat-2.2.4-h8f26bf8_1.tar.bz2"
  },
  "fastcache-1.0.2-py36h8606a76_0": {
    "md5": "204321d5d1c08fba6dfb27384e3a8eca",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/fastcache-1.0.2-py36h8606a76_0.tar.bz2"
  },
  "filelock-2.0.12-py36h0d0b4fb_0": {
    "md5": "27cccc60cdb002a46eb8aa80f32d3518",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/filelock-2.0.12-py36h0d0b4fb_0.tar.bz2"
  },
  "flask-0.12.2-py36h5658096_0": {
    "md5": "86bd1d3d995622ffddb98e373a2e8ce3",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/flask-0.12.2-py36h5658096_0.tar.bz2"
  },
  "flask-cors-3.0.3-py36h7387b97_0": {
    "md5": "e177957c6101828e954b04bee257a926",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/flask-cors-3.0.3-py36h7387b97_0.tar.bz2"
  },
  "freetype-2.8-h143eb01_0": {
    "md5": "c27644516abaab419897ff4d541ac3ba",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/freetype-2.8-h143eb01_0.tar.bz2"
  },
  "get_terminal_size-1.0.0-h7520d66_0": {
    "md5": "77ac7a22b3f4da13f9e52a2efd997c84",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/get_terminal_size-1.0.0-h7520d66_0.tar.bz2"
  },
  "gettext-0.19.8.1-hb0f4f8b_2": {
    "md5": "5166e68ea050d39c5a29bcf755b7c4eb",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/gettext-0.19.8.1-hb0f4f8b_2.tar.bz2"
  },
  "gevent-1.2.2-py36ha70b9d6_0": {
    "md5": "4866af3980e3bfdf922015f6ff6aedbb",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/gevent-1.2.2-py36ha70b9d6_0.tar.bz2"
  },
  "glib-2.53.6-ha08cb78_1": {
    "md5": "aed65cd36e7a36512a38be146cf77640",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/glib-2.53.6-ha08cb78_1.tar.bz2"
  },
  "glob2-0.5-py36h12393a9_0": {
    "md5": "de3980a4e2f656c72e730a8f53a4f7bf",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/glob2-0.5-py36h12393a9_0.tar.bz2"
  },
  "gmp-6.1.2-h4a9834d_0": {
    "md5": "ec83ddc711039c511eb7a448e3a3e9c6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/gmp-6.1.2-h4a9834d_0.tar.bz2"
  },
  "gmpy2-2.0.8-py36h7ef02cb_1": {
    "md5": "adc3bd311ef9d1c4d57ec53826bb95db",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/gmpy2-2.0.8-py36h7ef02cb_1.tar.bz2"
  },
  "greenlet-0.4.12-py36hf09ba7b_0": {
    "md5": "d84c6e8d034ecff58cce02dea399a80f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/greenlet-0.4.12-py36hf09ba7b_0.tar.bz2"
  },
  "h5py-2.7.0-py36h6400cee_1": {
    "md5": "800192b1cd9a9596db0233ddac26cbd2",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/h5py-2.7.0-py36h6400cee_1.tar.bz2"
  },
  "hdf5-1.10.1-h6090a45_0": {
    "md5": "c0f1398e3efb514aea414c667f787bff",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/hdf5-1.10.1-h6090a45_0.tar.bz2"
  },
  "heapdict-1.0.0-py36h27a9ac6_0": {
    "md5": "38c3ee348252b5389c7347d8d6f01236",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/heapdict-1.0.0-py36h27a9ac6_0.tar.bz2"
  },
  "html5lib-0.999999999-py36h79312fd_0": {
    "md5": "b5de1709a0f23909908b679e24dd91bc",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/html5lib-0.999999999-py36h79312fd_0.tar.bz2"
  },
  "icu-58.2-hea21ae5_0": {
    "md5": "fdb9f8f4d6529c1d66f7c62762dbb9e9",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/icu-58.2-hea21ae5_0.tar.bz2"
  },
  "idna-2.6-py36h8628d0a_1": {
    "md5": "61d7c7e4178bb6db0e881aaefe95825b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/idna-2.6-py36h8628d0a_1.tar.bz2"
  },
  "imageio-2.2.0-py36h5e01289_0": {
    "md5": "49066c0a35672ac5f68b485c1f616458",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/imageio-2.2.0-py36h5e01289_0.tar.bz2"
  },
  "imagesize-0.7.1-py36h3495948_0": {
    "md5": "416dacf285f2773907d33a4a1ecea0ce",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/imagesize-0.7.1-py36h3495948_0.tar.bz2"
  },
  "intel-openmp-2018.0.0-h68bdfb3_7": {
    "md5": "d1390f6e449e855e96a190396f5bddfe",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/intel-openmp-2018.0.0-h68bdfb3_7.tar.bz2"
  },
  "ipykernel-4.6.1-py36h3208c25_0": {
    "md5": "ac2ea79d19ad8f6f07aed285b75e6def",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ipykernel-4.6.1-py36h3208c25_0.tar.bz2"
  },
  "ipython-6.1.0-py36hf612aae_1": {
    "md5": "32605c7e03f6044756e0b0a81c64f98b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ipython-6.1.0-py36hf612aae_1.tar.bz2"
  },
  "ipython_genutils-0.2.0-py36h241746c_0": {
    "md5": "8bf5675df5be962037c24a345eff602c",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ipython_genutils-0.2.0-py36h241746c_0.tar.bz2"
  },
  "ipywidgets-7.0.0-py36h24d3910_0": {
    "md5": "59c3b50db2b173d81d6d20d1f9deb250",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ipywidgets-7.0.0-py36h24d3910_0.tar.bz2"
  },
  "isort-4.2.15-py36hceb2a01_0": {
    "md5": "755264f31d98c7dffa7779013593090f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/isort-4.2.15-py36hceb2a01_0.tar.bz2"
  },
  "itsdangerous-0.24-py36h49fbb8d_1": {
    "md5": "113976dd6e039096bd1cc74ac7ef47e7",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/itsdangerous-0.24-py36h49fbb8d_1.tar.bz2"
  },
  "jbig-2.1-h4d881f8_0": {
    "md5": "46d73d4693f37ce7455d1c01677165fa",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jbig-2.1-h4d881f8_0.tar.bz2"
  },
  "jdcal-1.3-py36h1986823_0": {
    "md5": "a565ec57d7cf1b2e2746cbe9ce2c24a4",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jdcal-1.3-py36h1986823_0.tar.bz2"
  },
  "jedi-0.10.2-py36h6325097_0": {
    "md5": "55f5e8ffa4b91952816e23501e593b2d",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jedi-0.10.2-py36h6325097_0.tar.bz2"
  },
  "jinja2-2.9.6-py36hde4beb4_1": {
    "md5": "f8641ca713bc0e923a0686ad808c5084",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jinja2-2.9.6-py36hde4beb4_1.tar.bz2"
  },
  "jpeg-9b-haccd157_1": {
    "md5": "7ac86b3837525fd0f321ba67c97ca966",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jpeg-9b-haccd157_1.tar.bz2"
  },
  "jsonschema-2.6.0-py36hb385e00_0": {
    "md5": "0ba384595487c950afdd0b8aa81e9a54",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jsonschema-2.6.0-py36hb385e00_0.tar.bz2"
  },
  "jupyter-1.0.0-py36h598a6cc_0": {
    "md5": "02263a0f23d8d37715b88a888382b0a9",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jupyter-1.0.0-py36h598a6cc_0.tar.bz2"
  },
  "jupyter_client-5.1.0-py36hf6c435f_0": {
    "md5": "b6b834e8db964ad361ba9acf09499328",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jupyter_client-5.1.0-py36hf6c435f_0.tar.bz2"
  },
  "jupyter_console-5.2.0-py36hccf5b1c_1": {
    "md5": "774625a3a5b73f9c01e7f5d2bf2edbb4",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jupyter_console-5.2.0-py36hccf5b1c_1.tar.bz2"
  },
  "jupyter_core-4.3.0-py36h93810fe_0": {
    "md5": "7842f154838ec163b482621ed02f3331",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jupyter_core-4.3.0-py36h93810fe_0.tar.bz2"
  },
  "jupyterlab-0.27.0-py36hd3092eb_2": {
    "md5": "051c85588596ad988b19258a46b1863c",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jupyterlab-0.27.0-py36hd3092eb_2.tar.bz2"
  },
  "jupyterlab_launcher-0.4.0-py36h93e02e9_0": {
    "md5": "7eed64d204b1ee5f8dca8a067d5563f0",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/jupyterlab_launcher-0.4.0-py36h93e02e9_0.tar.bz2"
  },
  "lazy-object-proxy-1.3.1-py36h2fbbe47_0": {
    "md5": "b676ab10ae735930f235305ed75d7899",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/lazy-object-proxy-1.3.1-py36h2fbbe47_0.tar.bz2"
  },
  "libcxx-4.0.1-h579ed51_0": {
    "md5": "291e61684d014641092020bdb1acb096",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libcxx-4.0.1-h579ed51_0.tar.bz2"
  },
  "libcxxabi-4.0.1-hebd6815_0": {
    "md5": "73ee4e71dae58f5173d649952c4c2b35",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libcxxabi-4.0.1-hebd6815_0.tar.bz2"
  },
  "libedit-3.1-hb4e282d_0": {
    "md5": "c410ee64ef02ccb2b65ddfed8e149638",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libedit-3.1-hb4e282d_0.tar.bz2"
  },
  "libffi-3.2.1-hd939716_3": {
    "md5": "7acc73020a451cb494564eb0dccd1e8b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libffi-3.2.1-hd939716_3.tar.bz2"
  },
  "libgfortran-3.0.1-h93005f0_2": {
    "md5": "e3a6e2b1a232418bbac9c47866273bdd",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libgfortran-3.0.1-h93005f0_2.tar.bz2"
  },
  "libiconv-1.15-h99df5da_5": {
    "md5": "5d8c22e60d5b51d950689728ffba1338",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libiconv-1.15-h99df5da_5.tar.bz2"
  },
  "libpng-1.6.32-hce72d48_2": {
    "md5": "710759242bf7333b95c157a7676a6ac3",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libpng-1.6.32-hce72d48_2.tar.bz2"
  },
  "libsodium-1.0.13-hba5e272_2": {
    "md5": "1a51117533060f83d325ef0ebfb0081a",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libsodium-1.0.13-hba5e272_2.tar.bz2"
  },
  "libssh2-1.8.0-h1218725_2": {
    "md5": "0d6b5ae4c0fd0a4442d8398dc1c7dbdb",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libssh2-1.8.0-h1218725_2.tar.bz2"
  },
  "libtiff-4.0.8-h8cd0352_9": {
    "md5": "a120b1d9e6dbee461c6161facc645ea9",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libtiff-4.0.8-h8cd0352_9.tar.bz2"
  },
  "libxml2-2.9.4-hbd0960b_5": {
    "md5": "f938da4be1c7adcf6b4e62e3f3e17bb8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libxml2-2.9.4-hbd0960b_5.tar.bz2"
  },
  "libxslt-1.1.29-h95a2935_5": {
    "md5": "9214dea4e6a54ac2ee9efd4ca680a25d",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/libxslt-1.1.29-h95a2935_5.tar.bz2"
  },
  "llvmlite-0.20.0-py36_0": {
    "md5": "6dd27ed7cea12ecd4f1e9fc266eee6a8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/llvmlite-0.20.0-py36_0.tar.bz2"
  },
  "locket-0.2.0-py36hca03003_1": {
    "md5": "475c1eb15e4203403e19ec1de7cbebbf",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/locket-0.2.0-py36hca03003_1.tar.bz2"
  },
  "lxml-4.1.0-py36h8179fc0_0": {
    "md5": "3e67cd69434110bd7ed936ea507c234d",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/lxml-4.1.0-py36h8179fc0_0.tar.bz2"
  },
  "lzo-2.10-hb6b8854_1": {
    "md5": "706f25185fcd6a05bbf95b90507473b1",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/lzo-2.10-hb6b8854_1.tar.bz2"
  },
  "markupsafe-1.0-py36h3a1e703_1": {
    "md5": "6ac9a14454235e1e0c59a85654b949d4",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/markupsafe-1.0-py36h3a1e703_1.tar.bz2"
  },
  "matplotlib-2.1.0-py36h5068139_0": {
    "md5": "2b600ad62d98403103768e56916b419c",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/matplotlib-2.1.0-py36h5068139_0.tar.bz2"
  },
  "mccabe-0.6.1-py36hdaeb55d_0": {
    "md5": "f75b3414cfcef1ae150772b9685f36c8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/mccabe-0.6.1-py36hdaeb55d_0.tar.bz2"
  },
  "mistune-0.7.4-py36hccd6237_0": {
    "md5": "256ce0e8abbd97057092e95690d004f3",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/mistune-0.7.4-py36hccd6237_0.tar.bz2"
  },
  "mkl-2018.0.0-h5ef208c_6": {
    "md5": "ce859b66763d5a159902d8fa52a2ee10",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/mkl-2018.0.0-h5ef208c_6.tar.bz2"
  },
  "mkl-service-1.1.2-py36h7ea6df4_4": {
    "md5": "0c5a9abfb94f03ef41e7e22b7575d3c1",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/mkl-service-1.1.2-py36h7ea6df4_4.tar.bz2"
  },
  "mpc-1.0.3-hc455b36_4": {
    "md5": "2c29c600d873031b981637d3d5034b74",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/mpc-1.0.3-hc455b36_4.tar.bz2"
  },
  "mpfr-3.1.5-h7fa3772_1": {
    "md5": "1da0dddd5242c6c9a467dc7a75f20a76",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/mpfr-3.1.5-h7fa3772_1.tar.bz2"
  },
  "mpmath-0.19-py36h9185fea_2": {
    "md5": "4d40f8c0845ba3bd3f9eea6242b99449",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/mpmath-0.19-py36h9185fea_2.tar.bz2"
  },
  "msgpack-python-0.4.8-py36h46767b2_0": {
    "md5": "b8eb6bf2e36ecfe581485178deb92c02",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/msgpack-python-0.4.8-py36h46767b2_0.tar.bz2"
  },
  "multipledispatch-0.4.9-py36hc5f92b5_0": {
    "md5": "909e502de33b3fafd8d7f42a5ae4b5b9",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/multipledispatch-0.4.9-py36hc5f92b5_0.tar.bz2"
  },
  "navigator-updater-0.1.0-py36h7aee5fb_0": {
    "md5": "27304696745f07e770ba38524b66e968",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/navigator-updater-0.1.0-py36h7aee5fb_0.tar.bz2"
  },
  "nbconvert-5.3.1-py36h810822e_0": {
    "md5": "7116229b8f8f3b1ce379f28f181f9d12",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/nbconvert-5.3.1-py36h810822e_0.tar.bz2"
  },
  "nbformat-4.4.0-py36h827af21_0": {
    "md5": "2fd10f01c32275cb652c1efccb2a0290",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/nbformat-4.4.0-py36h827af21_0.tar.bz2"
  },
  "ncurses-6.0-ha932d30_1": {
    "md5": "35a3941c125647a53c026987ece35006",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ncurses-6.0-ha932d30_1.tar.bz2"
  },
  "networkx-2.0-py36hefccab9_0": {
    "md5": "c0d2f18825814971e62e2138aa52f575",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/networkx-2.0-py36hefccab9_0.tar.bz2"
  },
  "nltk-3.2.4-py36h27d1ea0_0": {
    "md5": "6fea0e38ac04833597a850fb31247200",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/nltk-3.2.4-py36h27d1ea0_0.tar.bz2"
  },
  "nose-1.3.7-py36h73fae2b_2": {
    "md5": "323beefa9e4edd1fbaacdf8122410c2f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/nose-1.3.7-py36h73fae2b_2.tar.bz2"
  },
  "notebook-5.0.0-py36h462289e_2": {
    "md5": "42c1587e3e9b78ff2473cefb8b0d5041",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/notebook-5.0.0-py36h462289e_2.tar.bz2"
  },
  "numba-0.35.0-np113py36_6": {
    "md5": "8404fc0258fb30be4b3bf55ec9c70199",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/numba-0.35.0-np113py36_6.tar.bz2"
  },
  "numexpr-2.6.2-py36h0f4f1da_1": {
    "md5": "064940b213be01b611ffef4ed9bd44af",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/numexpr-2.6.2-py36h0f4f1da_1.tar.bz2"
  },
  "numpy-1.13.3-py36h2cdce51_0": {
    "md5": "69fb72bfb6212441307d23c922b421cb",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/numpy-1.13.3-py36h2cdce51_0.tar.bz2"
  },
  "numpydoc-0.7.0-py36he54d08e_0": {
    "md5": "e57d223688dad2b7fb4c708d49e8aaa5",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/numpydoc-0.7.0-py36he54d08e_0.tar.bz2"
  },
  "odo-0.5.1-py36hc1af34a_0": {
    "md5": "1d7f66c2d38a7bb51ca713bd9b7e9eb1",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/odo-0.5.1-py36hc1af34a_0.tar.bz2"
  },
  "olefile-0.44-py36ha08bf50_0": {
    "md5": "97d8282adb1410cfab5039fa441992ea",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/olefile-0.44-py36ha08bf50_0.tar.bz2"
  },
  "openpyxl-2.4.8-py36he899640_1": {
    "md5": "ddb26406285cea6ee7885cd329d944ec",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/openpyxl-2.4.8-py36he899640_1.tar.bz2"
  },
  "openssl-1.0.2l-h57f3a61_2": {
    "md5": "7a627b24441a2c0e6ca322710ff0fbae",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/openssl-1.0.2l-h57f3a61_2.tar.bz2"
  },
  "packaging-16.8-py36he5e8135_0": {
    "md5": "b1f5f2b66084f107e8f00520b3726e25",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/packaging-16.8-py36he5e8135_0.tar.bz2"
  },
  "pandas-0.20.3-py36hd6655d8_2": {
    "md5": "2fede35d4d151ae23ce68e202f71794e",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pandas-0.20.3-py36hd6655d8_2.tar.bz2"
  },
  "pandoc-1.19.2.1-ha5e8f32_1": {
    "md5": "f6e369d599448e3052f712ca9c3d9398",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pandoc-1.19.2.1-ha5e8f32_1.tar.bz2"
  },
  "pandocfilters-1.4.2-py36h3b0b094_1": {
    "md5": "c1d8d6de20078b3778c886ef2bb063f1",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pandocfilters-1.4.2-py36h3b0b094_1.tar.bz2"
  },
  "partd-0.3.8-py36hf5c4cb8_0": {
    "md5": "96dff484fedeae39c37a353f22d89c11",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/partd-0.3.8-py36hf5c4cb8_0.tar.bz2"
  },
  "path.py-10.3.1-py36hd33c240_0": {
    "md5": "fbc4cbdc1b4f5bcb20d6b2e1a6c2d2f4",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/path.py-10.3.1-py36hd33c240_0.tar.bz2"
  },
  "pathlib2-2.3.0-py36h877a6d8_0": {
    "md5": "b2dd1dda2f0824ca0b23d559740a2250",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pathlib2-2.3.0-py36h877a6d8_0.tar.bz2"
  },
  "patsy-0.4.1-py36ha1b3fa5_0": {
    "md5": "3747e6b6e2b5035728badc7f05b10507",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/patsy-0.4.1-py36ha1b3fa5_0.tar.bz2"
  },
  "pcre-8.41-h29eefc5_0": {
    "md5": "855674b5414399629741f0d8667b3198",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pcre-8.41-h29eefc5_0.tar.bz2"
  },
  "pep8-1.7.0-py36hc268eb1_0": {
    "md5": "f58eb6311c419ed004796bff45e20d91",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pep8-1.7.0-py36hc268eb1_0.tar.bz2"
  },
  "pexpect-4.2.1-py36h3eac828_0": {
    "md5": "2d0fdc41c06569d3e1522c530cdde470",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pexpect-4.2.1-py36h3eac828_0.tar.bz2"
  },
  "pickleshare-0.7.4-py36hf512f8e_0": {
    "md5": "f9f0840ecf910f2f284072bed566a3c2",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pickleshare-0.7.4-py36hf512f8e_0.tar.bz2"
  },
  "pillow-4.2.1-py36h0263179_0": {
    "md5": "894ce13671a2e89911b0d3b1bd316d76",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pillow-4.2.1-py36h0263179_0.tar.bz2"
  },
  "pip-9.0.1-py36hbd95645_3": {
    "md5": "a17ce99727402e364418cd277095fcd5",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pip-9.0.1-py36hbd95645_3.tar.bz2"
  },
  "pkginfo-1.4.1-py36h25bf955_0": {
    "md5": "7ca8c66caa8d7366388172a54aedb2b8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pkginfo-1.4.1-py36h25bf955_0.tar.bz2"
  },
  "ply-3.10-py36h10e714e_0": {
    "md5": "53a123f26957a3094fc44ba502cdbcfa",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ply-3.10-py36h10e714e_0.tar.bz2"
  },
  "prompt_toolkit-1.0.15-py36haeda067_0": {
    "md5": "eea86983b4e8ee5ac335246f5cee2e0f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/prompt_toolkit-1.0.15-py36haeda067_0.tar.bz2"
  },
  "psutil-5.4.0-py36ha052210_0": {
    "md5": "7104eec030cc0c8ce3f73cdb81c41e2f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/psutil-5.4.0-py36ha052210_0.tar.bz2"
  },
  "ptyprocess-0.5.2-py36he6521c3_0": {
    "md5": "64a83672184a1999bdfb43967570f268",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ptyprocess-0.5.2-py36he6521c3_0.tar.bz2"
  },
  "py-1.4.34-py36hecf431b_1": {
    "md5": "263977f917a35f7427d386039bfa1dc8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/py-1.4.34-py36hecf431b_1.tar.bz2"
  },
  "pycodestyle-2.3.1-py36h83e8646_0": {
    "md5": "003fa191d6a2bdb0fd0eee82064f15b9",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pycodestyle-2.3.1-py36h83e8646_0.tar.bz2"
  },
  "pycosat-0.6.2-py36h1486600_0": {
    "md5": "d1d9590f1fad1544041ed2bab3ffa3f6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pycosat-0.6.2-py36h1486600_0.tar.bz2"
  },
  "pycparser-2.18-py36h724b2fc_1": {
    "md5": "76fc7c10814ea37f623dfaa012b9b415",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pycparser-2.18-py36h724b2fc_1.tar.bz2"
  },
  "pycrypto-2.6.1-py36h72f2894_1": {
    "md5": "d98f9e11eae10d3b308c79811f28d022",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pycrypto-2.6.1-py36h72f2894_1.tar.bz2"
  },
  "pycurl-7.43.0-py36hdb90038_3": {
    "md5": "5089a7a8269dfa76e45a7bcbd6db3deb",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pycurl-7.43.0-py36hdb90038_3.tar.bz2"
  },
  "pyflakes-1.6.0-py36hea45e83_0": {
    "md5": "291055fba46e8e5d9b98cf81413cbf02",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pyflakes-1.6.0-py36hea45e83_0.tar.bz2"
  },
  "pygments-2.2.0-py36h240cd3f_0": {
    "md5": "8cab011578d68600a2b2f37e82439b2f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pygments-2.2.0-py36h240cd3f_0.tar.bz2"
  },
  "pylint-1.7.4-py36hdee9077_0": {
    "md5": "3acdf1bd3168ba9e22020d59e171bbd3",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pylint-1.7.4-py36hdee9077_0.tar.bz2"
  },
  "pyodbc-4.0.17-py36h5478161_0": {
    "md5": "7e0af542502cc5276785db8da2cf77db",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pyodbc-4.0.17-py36h5478161_0.tar.bz2"
  },
  "pyopenssl-17.2.0-py36h5d7bf08_0": {
    "md5": "1b7fad3669977a8ba9f87435d61db911",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pyopenssl-17.2.0-py36h5d7bf08_0.tar.bz2"
  },
  "pyparsing-2.2.0-py36hb281f35_0": {
    "md5": "d34ad8c2fb539887a3c6a548bd4cb332",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pyparsing-2.2.0-py36hb281f35_0.tar.bz2"
  },
  "pyqt-5.6.0-py36he5c6137_6": {
    "md5": "028c06cc3de55a80c42d8b0aa9ec6414",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pyqt-5.6.0-py36he5c6137_6.tar.bz2"
  },
  "pysocks-1.6.7-py36hfa33cec_1": {
    "md5": "9eef68553dd3e45da595e11c82c9262b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pysocks-1.6.7-py36hfa33cec_1.tar.bz2"
  },
  "pytables-3.4.2-py36hfbd7ab0_2": {
    "md5": "a80e3c7e8c1d142ab4e745d886d7f25d",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pytables-3.4.2-py36hfbd7ab0_2.tar.bz2"
  },
  "pytest-3.2.1-py36h9963153_1": {
    "md5": "947fc802ddfb938b1762435cc17db34f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pytest-3.2.1-py36h9963153_1.tar.bz2"
  },
  "python-3.6.3-h6804ab2_0": {
    "md5": "89ea000b0aab5c9e99f56dbd443aa32e",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/python-3.6.3-h6804ab2_0.tar.bz2"
  },
  "python-dateutil-2.6.1-py36h86d2abb_1": {
    "md5": "4e0fc19d49aaec8cc5efb4673d61efaf",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/python-dateutil-2.6.1-py36h86d2abb_1.tar.bz2"
  },
  "python.app-2-py36h7fe2238_6": {
    "md5": "dc380026d066fc5668c01094c2b42116",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/python.app-2-py36h7fe2238_6.tar.bz2"
  },
  "pytz-2017.2-py36h2e7dfbc_1": {
    "md5": "e02920347bb9142b836222105bd5b07e",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pytz-2017.2-py36h2e7dfbc_1.tar.bz2"
  },
  "pywavelets-0.5.2-py36h2710a04_0": {
    "md5": "021620fac7b73c9a7be76baa3fd88fa1",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pywavelets-0.5.2-py36h2710a04_0.tar.bz2"
  },
  "pyyaml-3.12-py36h2ba1e63_1": {
    "md5": "d74ec67dce5fdea0a20be4ec64963268",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pyyaml-3.12-py36h2ba1e63_1.tar.bz2"
  },
  "pyzmq-16.0.2-py36h087ffad_2": {
    "md5": "6181815b750dbf3094ed0a2af46e86af",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/pyzmq-16.0.2-py36h087ffad_2.tar.bz2"
  },
  "qt-5.6.2-h9975529_14": {
    "md5": "b62f7d0433149f5fddbac8b77cc6f909",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/qt-5.6.2-h9975529_14.tar.bz2"
  },
  "qtawesome-0.4.4-py36h468c6fb_0": {
    "md5": "aa91e1449231653c74e25a84b952d71d",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/qtawesome-0.4.4-py36h468c6fb_0.tar.bz2"
  },
  "qtconsole-4.3.1-py36hd96c0ff_0": {
    "md5": "aa6735ac51418287d3d1995cecb41140",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/qtconsole-4.3.1-py36hd96c0ff_0.tar.bz2"
  },
  "qtpy-1.3.1-py36h16bb863_0": {
    "md5": "6a0bcd6c0385c6b6c33bce81922f2447",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/qtpy-1.3.1-py36h16bb863_0.tar.bz2"
  },
  "readline-7.0-h81b24a6_3": {
    "md5": "10c2186657a2b795fac6e7f555967309",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/readline-7.0-h81b24a6_3.tar.bz2"
  },
  "requests-2.18.4-py36h4516966_1": {
    "md5": "68e58c0141573b1f55909ef2989169e7",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/requests-2.18.4-py36h4516966_1.tar.bz2"
  },
  "rope-0.10.5-py36h5764ad1_0": {
    "md5": "17f225d6bf14e5afcbc0a13fa1b4e10a",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/rope-0.10.5-py36h5764ad1_0.tar.bz2"
  },
  "ruamel_yaml-0.11.14-py36h9d7ade0_2": {
    "md5": "a3fded7764802311b389aa56ec8e8d16",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/ruamel_yaml-0.11.14-py36h9d7ade0_2.tar.bz2"
  },
  "scikit-image-0.13.0-py36h398857d_1": {
    "md5": "d294d04b7e6a6aac21918a34909b2e8b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/scikit-image-0.13.0-py36h398857d_1.tar.bz2"
  },
  "scikit-learn-0.19.1-py36hffbff8c_0": {
    "md5": "f173b7ca78f14fed230662ab0162dbbc",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/scikit-learn-0.19.1-py36hffbff8c_0.tar.bz2"
  },
  "scipy-0.19.1-py36h3e758e1_3": {
    "md5": "3510e9ad484007622aacaafe9835a839",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/scipy-0.19.1-py36h3e758e1_3.tar.bz2"
  },
  "seaborn-0.8.0-py36h74df97e_0": {
    "md5": "5054964050a65410fbeca2d7f3db472a",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/seaborn-0.8.0-py36h74df97e_0.tar.bz2"
  },
  "setuptools-36.5.0-py36h2134326_0": {
    "md5": "f3688cc69884e25dea591832bca26d01",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/setuptools-36.5.0-py36h2134326_0.tar.bz2"
  },
  "simplegeneric-0.8.1-py36he5b5b09_0": {
    "md5": "8a63a4e51e9c64e0f8bdfab247997e18",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/simplegeneric-0.8.1-py36he5b5b09_0.tar.bz2"
  },
  "singledispatch-3.4.0.3-py36hf20db9d_0": {
    "md5": "9da10384321a19d687b3da0cfa17240f",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/singledispatch-3.4.0.3-py36hf20db9d_0.tar.bz2"
  },
  "sip-4.18.1-py36h2824476_2": {
    "md5": "87eb1a81da0153bc8cb8155d9df98ab4",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sip-4.18.1-py36h2824476_2.tar.bz2"
  },
  "six-1.11.0-py36h0e22d5e_1": {
    "md5": "1b26af7a0b743ffd173b88a93ed23728",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/six-1.11.0-py36h0e22d5e_1.tar.bz2"
  },
  "snowballstemmer-1.2.1-py36h6c7b616_0": {
    "md5": "31e8026b27db78d9c169bd71e0adf8ec",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/snowballstemmer-1.2.1-py36h6c7b616_0.tar.bz2"
  },
  "sortedcollections-0.5.3-py36he9c3ed6_0": {
    "md5": "0a8ae267b734e651cb33a3ada4b4a3b6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sortedcollections-0.5.3-py36he9c3ed6_0.tar.bz2"
  },
  "sortedcontainers-1.5.7-py36ha982688_0": {
    "md5": "b137df357f227dd7994e50b9a91afb56",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sortedcontainers-1.5.7-py36ha982688_0.tar.bz2"
  },
  "sphinx-1.6.3-py36hcd1b3e7_0": {
    "md5": "73a1d6e860334d17f77e78338ad606fa",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sphinx-1.6.3-py36hcd1b3e7_0.tar.bz2"
  },
  "sphinxcontrib-1.0-py36h9364dc8_1": {
    "md5": "8e94a8edfb4dcf961b23708caa1724a4",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sphinxcontrib-1.0-py36h9364dc8_1.tar.bz2"
  },
  "sphinxcontrib-websupport-1.0.1-py36h92f4a7a_1": {
    "md5": "92116767b054eafcaaa2ed4cdf0ebf01",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sphinxcontrib-websupport-1.0.1-py36h92f4a7a_1.tar.bz2"
  },
  "spyder-3.2.4-py36h908396f_0": {
    "md5": "95422e31e2978a58fd41b5479f64f37b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/spyder-3.2.4-py36h908396f_0.tar.bz2"
  },
  "sqlalchemy-1.1.13-py36h156b851_0": {
    "md5": "2e8553d6578a4f0ed0558df14c519d3b",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sqlalchemy-1.1.13-py36h156b851_0.tar.bz2"
  },
  "sqlite-3.20.1-h900c3b0_1": {
    "md5": "4263ac4db9e250caba204919bebd0b64",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sqlite-3.20.1-h900c3b0_1.tar.bz2"
  },
  "statsmodels-0.8.0-py36h9c68fc9_0": {
    "md5": "8e780197de9fd237320c810960e16247",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/statsmodels-0.8.0-py36h9c68fc9_0.tar.bz2"
  },
  "sympy-1.1.1-py36h7f3cf04_0": {
    "md5": "360c185146eccbf0ba87fbd8bea9296d",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/sympy-1.1.1-py36h7f3cf04_0.tar.bz2"
  },
  "tblib-1.3.2-py36hda67792_0": {
    "md5": "6c07bc7f01244b95fb9a59c6bb3adfb0",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/tblib-1.3.2-py36hda67792_0.tar.bz2"
  },
  "terminado-0.6-py36h656782e_0": {
    "md5": "d8a1530a52651a9f088ceb50d013fc5a",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/terminado-0.6-py36h656782e_0.tar.bz2"
  },
  "testpath-0.3.1-py36h625a49b_0": {
    "md5": "580de06400036395c6f092357a877ad9",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/testpath-0.3.1-py36h625a49b_0.tar.bz2"
  },
  "tk-8.6.7-hcdce994_1": {
    "md5": "d3e4609fdea724d50ca69dcdf25a3fe6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/tk-8.6.7-hcdce994_1.tar.bz2"
  },
  "toolz-0.8.2-py36h7b95164_0": {
    "md5": "bb3208731af5e62e45c083446915abae",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/toolz-0.8.2-py36h7b95164_0.tar.bz2"
  },
  "tornado-4.5.2-py36h468dda9_0": {
    "md5": "6d32be696332ec2892dabc64c3655488",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/tornado-4.5.2-py36h468dda9_0.tar.bz2"
  },
  "traitlets-4.3.2-py36h65bd3ce_0": {
    "md5": "08b9fe712e97b063e5a6103e25bfb549",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/traitlets-4.3.2-py36h65bd3ce_0.tar.bz2"
  },
  "typing-3.6.2-py36haa2d9ef_0": {
    "md5": "c9bbfe27a84310aa5f73b702d1f4083e",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/typing-3.6.2-py36haa2d9ef_0.tar.bz2"
  },
  "unicodecsv-0.14.1-py36he531d66_0": {
    "md5": "1209c40cb6243b15427dac89d2350dd8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/unicodecsv-0.14.1-py36he531d66_0.tar.bz2"
  },
  "unixodbc-2.3.4-h4cb4dde_1": {
    "md5": "784af124861cc0627f6a5a58279e20a7",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/unixodbc-2.3.4-h4cb4dde_1.tar.bz2"
  },
  "urllib3-1.22-py36h68b9469_0": {
    "md5": "603417ae48ed5fa80d809dfb91c47f55",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/urllib3-1.22-py36h68b9469_0.tar.bz2"
  },
  "wcwidth-0.1.7-py36h8c6ec74_0": {
    "md5": "bc8c233e3237db0bc1f5f30fbaba3ef8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/wcwidth-0.1.7-py36h8c6ec74_0.tar.bz2"
  },
  "webencodings-0.5.1-py36h3b9701d_1": {
    "md5": "e42b45ef3ab7f63e91cb241ea14ee6da",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/webencodings-0.5.1-py36h3b9701d_1.tar.bz2"
  },
  "werkzeug-0.12.2-py36h168efa1_0": {
    "md5": "c8cffca811b74dfcc9a6a1145c3a0ae6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/werkzeug-0.12.2-py36h168efa1_0.tar.bz2"
  },
  "wheel-0.29.0-py36h3597b6d_1": {
    "md5": "5eb18a2f7dcb4f671edbb6bdf5d4ae96",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/wheel-0.29.0-py36h3597b6d_1.tar.bz2"
  },
  "widgetsnbextension-3.0.2-py36h91f43ea_1": {
    "md5": "ff703ac14501f464379559fb01ecafa5",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/widgetsnbextension-3.0.2-py36h91f43ea_1.tar.bz2"
  },
  "wrapt-1.10.11-py36hc29e774_0": {
    "md5": "52fd7fc91f94fca96b19dbbf1e914094",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/wrapt-1.10.11-py36hc29e774_0.tar.bz2"
  },
  "xlrd-1.1.0-py36h336f4a2_1": {
    "md5": "9633a175205afc292aa8401bdc148d53",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/xlrd-1.1.0-py36h336f4a2_1.tar.bz2"
  },
  "xlsxwriter-1.0.2-py36h3736301_0": {
    "md5": "530e2793e0e4ddc2918eafe8ececf581",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/xlsxwriter-1.0.2-py36h3736301_0.tar.bz2"
  },
  "xlwings-0.11.4-py36hc75f156_0": {
    "md5": "5060f37619f7caf035357106aa9431c3",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/xlwings-0.11.4-py36hc75f156_0.tar.bz2"
  },
  "xlwt-1.2.0-py36h5ad1178_0": {
    "md5": "a00e18aac4e5ca1fa2b124b0ee760491",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/xlwt-1.2.0-py36h5ad1178_0.tar.bz2"
  },
  "xz-5.2.3-ha24016e_1": {
    "md5": "00745240390c16e25596d35e24f2b064",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/xz-5.2.3-ha24016e_1.tar.bz2"
  },
  "yaml-0.1.7-hff548bb_1": {
    "md5": "0ee5bea24f06f545e7337766a4a6592c",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/yaml-0.1.7-hff548bb_1.tar.bz2"
  },
  "zeromq-4.2.2-h131e0f7_1": {
    "md5": "99183b87f3d9a37a9e4dd159561ae6d9",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/zeromq-4.2.2-h131e0f7_1.tar.bz2"
  },
  "zict-0.1.3-py36h71da714_0": {
    "md5": "6a66fc83d9c1b9bab428ee68c33122f8",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/zict-0.1.3-py36h71da714_0.tar.bz2"
  },
  "zlib-1.2.11-h60db283_1": {
    "md5": "bcd4679bf8147f0500f8948c1bde65d6",
    "url": "https://repo.continuum.io/pkgs/main/osx-64/zlib-1.2.11-h60db283_1.tar.bz2"
  }
}
C_ENVS = {
  "root": [
    "python-3.6.3-h6804ab2_0",
    "bzip2-1.0.6-h92991f9_1",
    "ca-certificates-2017.08.26-ha1e5d58_0",
    "conda-env-2.6.0-h36134e3_0",
    "gettext-0.19.8.1-hb0f4f8b_2",
    "intel-openmp-2018.0.0-h68bdfb3_7",
    "jbig-2.1-h4d881f8_0",
    "jpeg-9b-haccd157_1",
    "libcxxabi-4.0.1-hebd6815_0",
    "libgfortran-3.0.1-h93005f0_2",
    "libiconv-1.15-h99df5da_5",
    "libsodium-1.0.13-hba5e272_2",
    "libssh2-1.8.0-h1218725_2",
    "lzo-2.10-hb6b8854_1",
    "pandoc-1.19.2.1-ha5e8f32_1",
    "tk-8.6.7-hcdce994_1",
    "unixodbc-2.3.4-h4cb4dde_1",
    "xz-5.2.3-ha24016e_1",
    "yaml-0.1.7-hff548bb_1",
    "zlib-1.2.11-h60db283_1",
    "libcxx-4.0.1-h579ed51_0",
    "libpng-1.6.32-hce72d48_2",
    "mkl-2018.0.0-h5ef208c_6",
    "openssl-1.0.2l-h57f3a61_2",
    "curl-7.55.1-h7601780_3",
    "expat-2.2.4-h8f26bf8_1",
    "freetype-2.8-h143eb01_0",
    "gmp-6.1.2-h4a9834d_0",
    "hdf5-1.10.1-h6090a45_0",
    "icu-58.2-hea21ae5_0",
    "libffi-3.2.1-hd939716_3",
    "libtiff-4.0.8-h8cd0352_9",
    "ncurses-6.0-ha932d30_1",
    "pcre-8.41-h29eefc5_0",
    "zeromq-4.2.2-h131e0f7_1",
    "glib-2.53.6-ha08cb78_1",
    "libedit-3.1-hb4e282d_0",
    "libxml2-2.9.4-hbd0960b_5",
    "mpfr-3.1.5-h7fa3772_1",
    "readline-7.0-h81b24a6_3",
    "dbus-1.10.22-h50d9ad6_0",
    "libxslt-1.1.29-h95a2935_5",
    "mpc-1.0.3-hc455b36_4",
    "sqlite-3.20.1-h900c3b0_1",
    "qt-5.6.2-h9975529_14",
    "alabaster-0.7.10-py36h174008c_0",
    "appnope-0.1.0-py36hf537a9a_0",
    "appscript-1.0.1-py36h9e71e49_1",
    "asn1crypto-0.22.0-py36hb705621_1",
    "backports-1.0-py36ha3c1827_1",
    "beautifulsoup4-4.6.0-py36h72d3c9f_1",
    "bitarray-0.8.1-py36h20fa61d_0",
    "boto-2.48.0-py36hdbc59ac_1",
    "certifi-2017.7.27.1-py36hd973bb6_0",
    "chardet-3.0.4-py36h96c241c_1",
    "click-6.7-py36hec950be_0",
    "cloudpickle-0.4.0-py36h13b7e56_0",
    "colorama-0.3.9-py36hd29a30c_0",
    "contextlib2-0.5.5-py36hd66e5e7_0",
    "dask-core-0.15.3-py36hc0be6b7_0",
    "decorator-4.1.2-py36h69a1b52_0",
    "docutils-0.14-py36hbfde631_0",
    "entrypoints-0.2.3-py36hd81d71f_2",
    "et_xmlfile-1.0.1-py36h1315bdc_0",
    "fastcache-1.0.2-py36h8606a76_0",
    "filelock-2.0.12-py36h0d0b4fb_0",
    "glob2-0.5-py36h12393a9_0",
    "gmpy2-2.0.8-py36h7ef02cb_1",
    "greenlet-0.4.12-py36hf09ba7b_0",
    "heapdict-1.0.0-py36h27a9ac6_0",
    "idna-2.6-py36h8628d0a_1",
    "imagesize-0.7.1-py36h3495948_0",
    "ipython_genutils-0.2.0-py36h241746c_0",
    "itsdangerous-0.24-py36h49fbb8d_1",
    "jdcal-1.3-py36h1986823_0",
    "jedi-0.10.2-py36h6325097_0",
    "lazy-object-proxy-1.3.1-py36h2fbbe47_0",
    "llvmlite-0.20.0-py36_0",
    "locket-0.2.0-py36hca03003_1",
    "lxml-4.1.0-py36h8179fc0_0",
    "markupsafe-1.0-py36h3a1e703_1",
    "mccabe-0.6.1-py36hdaeb55d_0",
    "mistune-0.7.4-py36hccd6237_0",
    "mkl-service-1.1.2-py36h7ea6df4_4",
    "mpmath-0.19-py36h9185fea_2",
    "msgpack-python-0.4.8-py36h46767b2_0",
    "multipledispatch-0.4.9-py36hc5f92b5_0",
    "numpy-1.13.3-py36h2cdce51_0",
    "olefile-0.44-py36ha08bf50_0",
    "pandocfilters-1.4.2-py36h3b0b094_1",
    "path.py-10.3.1-py36hd33c240_0",
    "pep8-1.7.0-py36hc268eb1_0",
    "pickleshare-0.7.4-py36hf512f8e_0",
    "pkginfo-1.4.1-py36h25bf955_0",
    "ply-3.10-py36h10e714e_0",
    "psutil-5.4.0-py36ha052210_0",
    "ptyprocess-0.5.2-py36he6521c3_0",
    "py-1.4.34-py36hecf431b_1",
    "pycodestyle-2.3.1-py36h83e8646_0",
    "pycosat-0.6.2-py36h1486600_0",
    "pycparser-2.18-py36h724b2fc_1",
    "pycrypto-2.6.1-py36h72f2894_1",
    "pycurl-7.43.0-py36hdb90038_3",
    "pyodbc-4.0.17-py36h5478161_0",
    "pyparsing-2.2.0-py36hb281f35_0",
    "pysocks-1.6.7-py36hfa33cec_1",
    "python.app-2-py36h7fe2238_6",
    "pytz-2017.2-py36h2e7dfbc_1",
    "pyyaml-3.12-py36h2ba1e63_1",
    "pyzmq-16.0.2-py36h087ffad_2",
    "qtpy-1.3.1-py36h16bb863_0",
    "rope-0.10.5-py36h5764ad1_0",
    "ruamel_yaml-0.11.14-py36h9d7ade0_2",
    "simplegeneric-0.8.1-py36he5b5b09_0",
    "sip-4.18.1-py36h2824476_2",
    "six-1.11.0-py36h0e22d5e_1",
    "snowballstemmer-1.2.1-py36h6c7b616_0",
    "sortedcontainers-1.5.7-py36ha982688_0",
    "sphinxcontrib-1.0-py36h9364dc8_1",
    "sqlalchemy-1.1.13-py36h156b851_0",
    "tblib-1.3.2-py36hda67792_0",
    "testpath-0.3.1-py36h625a49b_0",
    "toolz-0.8.2-py36h7b95164_0",
    "tornado-4.5.2-py36h468dda9_0",
    "typing-3.6.2-py36haa2d9ef_0",
    "unicodecsv-0.14.1-py36he531d66_0",
    "wcwidth-0.1.7-py36h8c6ec74_0",
    "webencodings-0.5.1-py36h3b9701d_1",
    "werkzeug-0.12.2-py36h168efa1_0",
    "wrapt-1.10.11-py36hc29e774_0",
    "xlrd-1.1.0-py36h336f4a2_1",
    "xlsxwriter-1.0.2-py36h3736301_0",
    "xlwt-1.2.0-py36h5ad1178_0",
    "babel-2.5.0-py36h9f161ff_0",
    "backports.shutil_get_terminal_size-1.0.0-py36hd7a2ee4_2",
    "bottleneck-1.2.1-py36hbd380ad_0",
    "cffi-1.10.0-py36h880867e_1",
    "conda-verify-2.0.0-py36he837df3_0",
    "cycler-0.10.0-py36hfc81398_0",
    "cytoolz-0.8.2-py36h290905f_0",
    "h5py-2.7.0-py36h6400cee_1",
    "html5lib-0.999999999-py36h79312fd_0",
    "networkx-2.0-py36hefccab9_0",
    "nltk-3.2.4-py36h27d1ea0_0",
    "numba-0.35.0-np113py36_6",
    "numexpr-2.6.2-py36h0f4f1da_1",
    "openpyxl-2.4.8-py36he899640_1",
    "packaging-16.8-py36he5e8135_0",
    "partd-0.3.8-py36hf5c4cb8_0",
    "pathlib2-2.3.0-py36h877a6d8_0",
    "pexpect-4.2.1-py36h3eac828_0",
    "pillow-4.2.1-py36h0263179_0",
    "pyqt-5.6.0-py36he5c6137_6",
    "python-dateutil-2.6.1-py36h86d2abb_1",
    "pywavelets-0.5.2-py36h2710a04_0",
    "qtawesome-0.4.4-py36h468c6fb_0",
    "scipy-0.19.1-py36h3e758e1_3",
    "setuptools-36.5.0-py36h2134326_0",
    "singledispatch-3.4.0.3-py36hf20db9d_0",
    "sortedcollections-0.5.3-py36he9c3ed6_0",
    "sphinxcontrib-websupport-1.0.1-py36h92f4a7a_1",
    "sympy-1.1.1-py36h7f3cf04_0",
    "terminado-0.6-py36h656782e_0",
    "traitlets-4.3.2-py36h65bd3ce_0",
    "xlwings-0.11.4-py36hc75f156_0",
    "zict-0.1.3-py36h71da714_0",
    "astroid-1.5.3-py36h1333018_0",
    "bleach-2.0.0-py36h8fcea71_0",
    "clyent-1.2.2-py36hae3ad88_0",
    "cryptography-2.0.3-py36h22d4226_1",
    "cython-0.26.1-py36hd51f8eb_0",
    "datashape-0.5.4-py36hfb22df8_0",
    "distributed-1.19.1-py36h4ae75d2_0",
    "get_terminal_size-1.0.0-h7520d66_0",
    "gevent-1.2.2-py36ha70b9d6_0",
    "imageio-2.2.0-py36h5e01289_0",
    "isort-4.2.15-py36hceb2a01_0",
    "jinja2-2.9.6-py36hde4beb4_1",
    "jsonschema-2.6.0-py36hb385e00_0",
    "jupyter_core-4.3.0-py36h93810fe_0",
    "matplotlib-2.1.0-py36h5068139_0",
    "navigator-updater-0.1.0-py36h7aee5fb_0",
    "nose-1.3.7-py36h73fae2b_2",
    "pandas-0.20.3-py36hd6655d8_2",
    "patsy-0.4.1-py36ha1b3fa5_0",
    "pyflakes-1.6.0-py36hea45e83_0",
    "pygments-2.2.0-py36h240cd3f_0",
    "pytables-3.4.2-py36hfbd7ab0_2",
    "pytest-3.2.1-py36h9963153_1",
    "scikit-learn-0.19.1-py36hffbff8c_0",
    "wheel-0.29.0-py36h3597b6d_1",
    "astropy-2.0.2-py36hf79c81d_4",
    "bkcharts-0.2-py36h073222e_0",
    "bokeh-0.12.10-py36hfd5be35_0",
    "flask-0.12.2-py36h5658096_0",
    "jupyter_client-5.1.0-py36hf6c435f_0",
    "nbformat-4.4.0-py36h827af21_0",
    "pip-9.0.1-py36hbd95645_3",
    "prompt_toolkit-1.0.15-py36haeda067_0",
    "pylint-1.7.4-py36hdee9077_0",
    "pyopenssl-17.2.0-py36h5d7bf08_0",
    "statsmodels-0.8.0-py36h9c68fc9_0",
    "dask-0.15.3-py36hc3ad2d6_0",
    "flask-cors-3.0.3-py36h7387b97_0",
    "ipython-6.1.0-py36hf612aae_1",
    "nbconvert-5.3.1-py36h810822e_0",
    "seaborn-0.8.0-py36h74df97e_0",
    "urllib3-1.22-py36h68b9469_0",
    "ipykernel-4.6.1-py36h3208c25_0",
    "odo-0.5.1-py36hc1af34a_0",
    "requests-2.18.4-py36h4516966_1",
    "scikit-image-0.13.0-py36h398857d_1",
    "anaconda-client-1.6.5-py36h04cfe59_0",
    "blaze-0.11.3-py36h02e7a37_0",
    "conda-4.3.30-py36h173c244_0",
    "jupyter_console-5.2.0-py36hccf5b1c_1",
    "notebook-5.0.0-py36h462289e_2",
    "qtconsole-4.3.1-py36hd96c0ff_0",
    "sphinx-1.6.3-py36hcd1b3e7_0",
    "anaconda-project-0.8.0-py36h99320b2_0",
    "conda-build-3.0.27-py36hb78d8cd_0",
    "jupyterlab_launcher-0.4.0-py36h93e02e9_0",
    "numpydoc-0.7.0-py36he54d08e_0",
    "widgetsnbextension-3.0.2-py36h91f43ea_1",
    "anaconda-navigator-1.6.9-py36ha31b149_0",
    "ipywidgets-7.0.0-py36h24d3910_0",
    "jupyterlab-0.27.0-py36hd3092eb_2",
    "spyder-3.2.4-py36h908396f_0",
    "_ipyw_jlab_nb_ext_conf-0.1.0-py36h2fc01ae_0",
    "jupyter-1.0.0-py36h598a6cc_0",
    "anaconda-5.0.1-py36h6e48e2d_1"
  ]
}



def _link(src, dst, linktype=LINK_HARD):
    if linktype == LINK_HARD:
        if on_win:
            from ctypes import windll, wintypes
            CreateHardLink = windll.kernel32.CreateHardLinkW
            CreateHardLink.restype = wintypes.BOOL
            CreateHardLink.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR,
                                       wintypes.LPVOID]
            if not CreateHardLink(dst, src, None):
                raise OSError('win32 hard link failed')
        else:
            os.link(src, dst)
    elif linktype == LINK_COPY:
        # copy relative symlinks as symlinks
        if islink(src) and not os.readlink(src).startswith(os.path.sep):
            os.symlink(os.readlink(src), dst)
        else:
            shutil.copy2(src, dst)
    else:
        raise Exception("Did not expect linktype=%r" % linktype)


def rm_rf(path):
    """
    try to delete path, but never fail
    """
    try:
        if islink(path) or isfile(path):
            # Note that we have to check if the destination is a link because
            # exists('/path/to/dead-link') will return False, although
            # islink('/path/to/dead-link') is True.
            os.unlink(path)
        elif isdir(path):
            shutil.rmtree(path)
    except (OSError, IOError):
        pass


def yield_lines(path):
    for line in open(path):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        yield line


prefix_placeholder = ('/opt/anaconda1anaconda2'
                      # this is intentionally split into parts,
                      # such that running this program on itself
                      # will leave it unchanged
                      'anaconda3')

def read_has_prefix(path):
    """
    reads `has_prefix` file and return dict mapping filenames to
    tuples(placeholder, mode)
    """
    import shlex

    res = {}
    try:
        for line in yield_lines(path):
            try:
                parts = [x.strip('"\'') for x in shlex.split(line, posix=False)]
                # assumption: placeholder and mode will never have a space
                placeholder, mode, f = parts[0], parts[1], ' '.join(parts[2:])
                res[f] = (placeholder, mode)
            except (ValueError, IndexError):
                res[line] = (prefix_placeholder, 'text')
    except IOError:
        pass
    return res


def exp_backoff_fn(fn, *args):
    """
    for retrying file operations that fail on Windows due to virus scanners
    """
    if not on_win:
        return fn(*args)

    import time
    import errno
    max_tries = 6  # max total time = 6.4 sec
    for n in range(max_tries):
        try:
            result = fn(*args)
        except (OSError, IOError) as e:
            if e.errno in (errno.EPERM, errno.EACCES):
                if n == max_tries - 1:
                    raise Exception("max_tries=%d reached" % max_tries)
                time.sleep(0.1 * (2 ** n))
            else:
                raise e
        else:
            return result


class PaddingError(Exception):
    pass


def binary_replace(data, a, b):
    """
    Perform a binary replacement of `data`, where the placeholder `a` is
    replaced with `b` and the remaining string is padded with null characters.
    All input arguments are expected to be bytes objects.
    """
    def replace(match):
        occurances = match.group().count(a)
        padding = (len(a) - len(b)) * occurances
        if padding < 0:
            raise PaddingError(a, b, padding)
        return match.group().replace(a, b) + b'\0' * padding

    pat = re.compile(re.escape(a) + b'([^\0]*?)\0')
    res = pat.sub(replace, data)
    assert len(res) == len(data)
    return res


def update_prefix(path, new_prefix, placeholder, mode):
    if on_win:
        # force all prefix replacements to forward slashes to simplify need
        # to escape backslashes - replace with unix-style path separators
        new_prefix = new_prefix.replace('\\', '/')

    path = os.path.realpath(path)
    with open(path, 'rb') as fi:
        data = fi.read()
    if mode == 'text':
        new_data = data.replace(placeholder.encode('utf-8'),
                                new_prefix.encode('utf-8'))
    elif mode == 'binary':
        if on_win:
            # anaconda-verify will not allow binary placeholder on Windows.
            # However, since some packages might be created wrong (and a
            # binary placeholder would break the package, we just skip here.
            return
        new_data = binary_replace(data, placeholder.encode('utf-8'),
                                  new_prefix.encode('utf-8'))
    else:
        sys.exit("Invalid mode:" % mode)

    if new_data == data:
        return
    st = os.lstat(path)
    # unlink in case the file is memory mapped
    exp_backoff_fn(os.unlink, path)
    with open(path, 'wb') as fo:
        fo.write(new_data)
    os.chmod(path, stat.S_IMODE(st.st_mode))


def name_dist(dist):
    if hasattr(dist, 'name'):
        return dist.name
    else:
        return dist.rsplit('-', 2)[0]


def create_meta(prefix, dist, info_dir, extra_info):
    """
    Create the conda metadata, in a given prefix, for a given package.
    """
    # read info/index.json first
    with open(join(info_dir, 'index.json')) as fi:
        meta = json.load(fi)
    # add extra info
    meta.update(extra_info)
    # write into <prefix>/conda-meta/<dist>.json
    meta_dir = join(prefix, 'conda-meta')
    if not isdir(meta_dir):
        os.makedirs(meta_dir)
        with open(join(meta_dir, 'history'), 'w') as fo:
            fo.write('')
    with open(join(meta_dir, dist + '.json'), 'w') as fo:
        json.dump(meta, fo, indent=2, sort_keys=True)


def run_script(prefix, dist, action='post-link'):
    """
    call the post-link (or pre-unlink) script, and return True on success,
    False on failure
    """
    path = join(prefix, 'Scripts' if on_win else 'bin', '.%s-%s.%s' % (
            name_dist(dist),
            action,
            'bat' if on_win else 'sh'))
    if not isfile(path):
        return True
    if SKIP_SCRIPTS:
        print("WARNING: skipping %s script by user request" % action)
        return True

    if on_win:
        try:
            args = [os.environ['COMSPEC'], '/c', path]
        except KeyError:
            return False
    else:
        shell_path = '/bin/sh' if 'bsd' in sys.platform else '/bin/bash'
        args = [shell_path, path]

    env = os.environ
    env['PREFIX'] = prefix

    import subprocess
    try:
        subprocess.check_call(args, env=env)
    except subprocess.CalledProcessError:
        return False
    return True


url_pat = re.compile(r'''
(?P<baseurl>\S+/)                 # base URL
(?P<fn>[^\s#/]+)                  # filename
([#](?P<md5>[0-9a-f]{32}))?       # optional MD5
$                                 # EOL
''', re.VERBOSE)

def read_urls(dist):
    try:
        data = open(join(PKGS_DIR, 'urls')).read()
        for line in data.split()[::-1]:
            m = url_pat.match(line)
            if m is None:
                continue
            if m.group('fn') == '%s.tar.bz2' % dist:
                return {'url': m.group('baseurl') + m.group('fn'),
                        'md5': m.group('md5')}
    except IOError:
        pass
    return {}


def read_no_link(info_dir):
    res = set()
    for fn in 'no_link', 'no_softlink':
        try:
            res.update(set(yield_lines(join(info_dir, fn))))
        except IOError:
            pass
    return res


def linked(prefix):
    """
    Return the (set of canonical names) of linked packages in prefix.
    """
    meta_dir = join(prefix, 'conda-meta')
    if not isdir(meta_dir):
        return set()
    return set(fn[:-5] for fn in os.listdir(meta_dir) if fn.endswith('.json'))


def link(prefix, dist, linktype=LINK_HARD, info_dir=None):
    '''
    Link a package in a specified prefix.  We assume that the packacge has
    been extra_info in either
      - <PKGS_DIR>/dist
      - <ROOT_PREFIX>/ (when the linktype is None)
    '''
    if linktype:
        source_dir = join(PKGS_DIR, dist)
        info_dir = join(source_dir, 'info')
        no_link = read_no_link(info_dir)
    else:
        info_dir = info_dir or join(prefix, 'info')

    files = list(yield_lines(join(info_dir, 'files')))
    # TODO: Use paths.json, if available or fall back to this method
    has_prefix_files = read_has_prefix(join(info_dir, 'has_prefix'))

    if linktype:
        for f in files:
            src = join(source_dir, f)
            dst = join(prefix, f)
            dst_dir = dirname(dst)
            if not isdir(dst_dir):
                os.makedirs(dst_dir)
            if exists(dst):
                if FORCE:
                    rm_rf(dst)
                else:
                    raise Exception("dst exists: %r" % dst)
            lt = linktype
            if f in has_prefix_files or f in no_link or islink(src):
                lt = LINK_COPY
            try:
                _link(src, dst, lt)
            except OSError:
                pass

    for f in sorted(has_prefix_files):
        placeholder, mode = has_prefix_files[f]
        try:
            update_prefix(join(prefix, f), prefix, placeholder, mode)
        except PaddingError:
            sys.exit("ERROR: placeholder '%s' too short in: %s\n" %
                     (placeholder, dist))

    if not run_script(prefix, dist, 'post-link'):
        sys.exit("Error: post-link failed for: %s" % dist)

    meta = {
        'files': files,
        'link': ({'source': source_dir,
                  'type': link_name_map.get(linktype)}
                 if linktype else None),
    }
    try:    # add URL and MD5
        meta.update(IDISTS[dist])
    except KeyError:
        meta.update(read_urls(dist))
    meta['installed_by'] = 'Anaconda3-5.0.1-MacOSX-x86_64.pkg'
    create_meta(prefix, dist, info_dir, meta)


def duplicates_to_remove(linked_dists, keep_dists):
    """
    Returns the (sorted) list of distributions to be removed, such that
    only one distribution (for each name) remains.  `keep_dists` is an
    interable of distributions (which are not allowed to be removed).
    """
    from collections import defaultdict

    keep_dists = set(keep_dists)
    ldists = defaultdict(set) # map names to set of distributions
    for dist in linked_dists:
        name = name_dist(dist)
        ldists[name].add(dist)

    res = set()
    for dists in ldists.values():
        # `dists` is the group of packages with the same name
        if len(dists) == 1:
            # if there is only one package, nothing has to be removed
            continue
        if dists & keep_dists:
            # if the group has packages which are have to be kept, we just
            # take the set of packages which are in group but not in the
            # ones which have to be kept
            res.update(dists - keep_dists)
        else:
            # otherwise, we take lowest (n-1) (sorted) packages
            res.update(sorted(dists)[:-1])
    return sorted(res)


def yield_idists():
    for line in open(join(PKGS_DIR, 'urls')):
        m = url_pat.match(line)
        if m:
            fn = m.group('fn')
            yield fn[:-8]


def remove_duplicates():
    idists = list(yield_idists())
    keep_files = set()
    for dist in idists:
        with open(join(ROOT_PREFIX, 'conda-meta', dist + '.json')) as fi:
            meta = json.load(fi)
        keep_files.update(meta['files'])

    for dist in duplicates_to_remove(linked(ROOT_PREFIX), idists):
        print("unlinking: %s" % dist)
        meta_path = join(ROOT_PREFIX, 'conda-meta', dist + '.json')
        with open(meta_path) as fi:
            meta = json.load(fi)
        for f in meta['files']:
            if f not in keep_files:
                rm_rf(join(ROOT_PREFIX, f))
        rm_rf(meta_path)


def determine_link_type_capability():
    src = join(PKGS_DIR, 'urls')
    dst = join(ROOT_PREFIX, '.hard-link')
    assert isfile(src), src
    assert not isfile(dst), dst
    try:
        _link(src, dst, LINK_HARD)
        linktype = LINK_HARD
    except OSError:
        linktype = LINK_COPY
    finally:
        rm_rf(dst)
    return linktype


def link_dist(dist, linktype=None):
    if not linktype:
        linktype = determine_link_type_capability()
    prefix = prefix_env('root')
    link(prefix, dist, linktype)


def link_idists():
    linktype = determine_link_type_capability()
    for env_name in sorted(C_ENVS):
        dists = C_ENVS[env_name]
        assert isinstance(dists, list)
        if len(dists) == 0:
            continue

        prefix = prefix_env(env_name)
        for dist in dists:
            assert dist in IDISTS
            link_dist(dist, linktype)

        for dist in duplicates_to_remove(linked(prefix), dists):
            meta_path = join(prefix, 'conda-meta', dist + '.json')
            print("WARNING: unlinking: %s" % meta_path)
            try:
                os.rename(meta_path, meta_path + '.bak')
            except OSError:
                rm_rf(meta_path)


def prefix_env(env_name):
    if env_name == 'root':
        return ROOT_PREFIX
    else:
        return join(ROOT_PREFIX, 'envs', env_name)


def post_extract(env_name='root'):
    """
    assuming that the package is extracted in the environment `env_name`,
    this function does everything link() does except the actual linking,
    i.e. update prefix files, run 'post-link', creates the conda metadata,
    and removed the info/ directory afterwards.
    """
    prefix = prefix_env(env_name)
    info_dir = join(prefix, 'info')
    with open(join(info_dir, 'index.json')) as fi:
        meta = json.load(fi)
    dist = '%(name)s-%(version)s-%(build)s' % meta
    if FORCE:
        run_script(prefix, dist, 'pre-unlink')
    link(prefix, dist, linktype=None)
    shutil.rmtree(info_dir)


def multi_post_extract():
    # This function is called when using the --multi option, when building
    # .pkg packages on OSX.  I tried avoiding this extra option by running
    # the post extract step on each individual package (like it is done for
    # the .sh and .exe installers), by adding a postinstall script to each
    # conda .pkg file, but this did not work as expected.  Running all the
    # post extracts at end is also faster and could be considered for the
    # other installer types as well.
    for dist in yield_idists():
        info_dir = join(ROOT_PREFIX, 'info', dist)
        with open(join(info_dir, 'index.json')) as fi:
            meta = json.load(fi)
        dist = '%(name)s-%(version)s-%(build)s' % meta
        link(ROOT_PREFIX, dist, linktype=None, info_dir=info_dir)


def main():
    global ROOT_PREFIX, PKGS_DIR

    p = OptionParser(description="conda link tool used by installers")

    p.add_option('--root-prefix',
                 action="store",
                 default=abspath(join(__file__, '..', '..')),
                 help="root prefix (defaults to %default)")

    p.add_option('--post',
                 action="store",
                 help="perform post extract (on a single package), "
                      "in environment NAME",
                 metavar='NAME')

    opts, args = p.parse_args()
    if args:
        p.error('no arguments expected')

    ROOT_PREFIX = opts.root_prefix.replace('//', '/')
    PKGS_DIR = join(ROOT_PREFIX, 'pkgs')

    if opts.post:
        post_extract(opts.post)
        return

    if FORCE:
        print("using -f (force) option")

    link_idists()


def main2():
    global SKIP_SCRIPTS, ROOT_PREFIX, PKGS_DIR

    p = OptionParser(description="conda post extract tool used by installers")

    p.add_option('--skip-scripts',
                 action="store_true",
                 help="skip running pre/post-link scripts")

    p.add_option('--rm-dup',
                 action="store_true",
                 help="remove duplicates")

    p.add_option('--multi',
                 action="store_true",
                 help="multi post extract usecase")

    p.add_option('--link-dist',
                 action="store",
                 default=None,
                 help="link dist")

    p.add_option('--root-prefix',
                 action="store",
                 default=abspath(join(__file__, '..', '..')),
                 help="root prefix (defaults to %default)")

    opts, args = p.parse_args()
    ROOT_PREFIX = opts.root_prefix.replace('//', '/')
    PKGS_DIR = join(ROOT_PREFIX, 'pkgs')

    if args:
        p.error('no arguments expected')

    if opts.skip_scripts:
        SKIP_SCRIPTS = True

    if opts.rm_dup:
        remove_duplicates()
        return

    if opts.multi:
        multi_post_extract()
        return

    if opts.link_dist:
        link_dist(opts.link_dist)
        return

    post_extract()


def warn_on_special_chrs():
    if on_win:
        return
    for c in SPECIAL_ASCII:
        if c in ROOT_PREFIX:
            print("WARNING: found '%s' in install prefix." % c)


if __name__ == '__main__':
    if IDISTS:
        main()
        warn_on_special_chrs()
    else: # common usecase
        main2()
