API Documentation
-----------------

.. currentmodule:: sqlakeyset

Pagination API
^^^^^^^^^^^^^^

.. autofunction:: select_page
.. autofunction:: sqlakeyset.asyncio.select_page
.. autofunction:: get_page

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

Custom Types in Bookmarks
^^^^^^^^^^^^^^^^^^^^^^^^^

If you're using custom types for your ordering columns, you will need to register them with sqlakeyset in order to use bookmarks.

.. autofunction:: custom_bookmark_type
