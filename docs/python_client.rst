Python client
-------------


.. todo::

   Python client intsallation and usage examples.








.. module:: client


.. autofunction:: connect

Use `connect` when you want to limit the livetime of a client connection to a
certain scope::

     with connect("localhost", 12345) as masenko:
         masenko.push("register-fruit", {"age": 2, "color": "blue"})

.. autoclass:: BareClient
   :members:
   :inherited-members:

.. autoclass:: Client
   :members:
   :inherited-members:

.. autoclass:: Transaction
   :members:


