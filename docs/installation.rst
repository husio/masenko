Installation
------------

Masenko can be installed in several ways.

.. todo::

   This section should describe installation from source, docker hub and release binary


Installing from source
======================

.. todo::

   Describe how to install from source.

Installing from Docker Registry
===============================

Masenko Docker image is available on GitHub Packages. Each release is
publishing a new image. Make sure to use the latest `VERSION` number available.

.. warning::

   Currently, GitHub Registry requires authentication which renders it useless
   for Open Source projects.

   https://github.community/t/docker-pull-from-public-github-package-registry-fail-with-no-basic-auth-credentials-error/16358

.. code:: sh

   $ export VERSION=v0.0.1
   $ docker pull docker.pkg.github.com/husio/masenko/masenko:$VERSION

Once the Docker image is pulled from the repository, run it locally using
desired configuration. For local development, you might want to use the below
setup:

.. code:: sh

   $ docker run --rm -it masenko -p 8000:8000 -p 12345:12345



Installing binary
=================

.. todo::

   Describe how to install by downloading a release binary.
