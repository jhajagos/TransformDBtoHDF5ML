from setuptools import setup

setup(name='prediction_matrix_generate',
      version="0.1",
      description='Tools for transforming database tables into HDF5 matrices for machine learning applications',
      url='https://github.com/jhajagos/TransformDBtoHDF5ML',
      packages=["prediction_matrix_generate"],
      license="Apache",
      author="Janos Hajagos",
      author_email="risk.limits@gmail.com",
      zip_safe=False,
      requires=["h5py", "sqlalchemy", "numpy"]
    )