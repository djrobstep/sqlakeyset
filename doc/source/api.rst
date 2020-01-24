API Documentation
-----------------

.. currentmodule:: sqlakeyset

Pagination API
^^^^^^^^^^^^^^

.. autofunction:: get_page
.. autofunction:: select_page

Pagination Results
^^^^^^^^^^^^^^^^^^

.. autoclass:: Page
   :members:
.. autoclass:: Paging
   :members:

Bookmark Serialization
^^^^^^^^^^^^^^^^^^^^^^

In most use cases, you shouldn't need to call these directly - bookmarks can be obtained by calling the `bookmark_`-prefixed methods on :class:`Page` objects, and can be passed directly as the `page`, `after` or `before` parameters to :func:`get_page` and :func:`select_page`.

.. autofunction:: serialize_bookmark
.. autofunction:: unserialize_bookmark

