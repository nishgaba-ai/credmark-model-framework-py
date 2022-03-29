# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'Credmark Model Framework'
copyright = '2022, Credmark'
author = 'Credmark'

release = '0.1'
version = '0.1.0'

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    "myst_parser",
]

myst_enable_extensions = [
    "deflist",
    "colon_fence",
]

autosummary_generate = True

autosummary_filename_map = {
    'credmark.types.data.account::IterableListGenericDTO[Account]':
    'credmark.types.data.account::IterableListGenericDTO_Account',
    'credmark.types.data.token::IterableListGenericDTO[Token]':
    'credmark.types.data.token::IterableListGenericDTO_Token'
}

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'
