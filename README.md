# TransformDBtoHDF5ML

The library transforms rows from a relational database table into a nested document and then to a standard 
matrix file format. The document structure consists of nested dictionaries and is formatted in a 
human readable JSON format. The self describing matrix format is HDF5 which can be read by a wide range
of scientific programming environments including: Matlab, scikits via h5py, Mathematica and R.  
This code started out as a mapper for relational data into a format that could be used to easily train 
machine learning algorithms for hospital readmission quality work. The examples in the tests are 
formatted around this use case. The two programs "build_document_mapping_from_db.py" 
and "build_hdf5_matrix_from_document.py" are not limited to the readmission use case and have 
been designed to scale with data size.
