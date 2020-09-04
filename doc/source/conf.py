# -*- coding: utf-8 -*-
#
# Ray documentation build configuration file, created by
# sphinx-quickstart on Fri Jul  1 13:19:58 2016.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

import glob
import shutil
import sys
import os
import urllib
sys.path.insert(0, os.path.abspath('.'))
from custom_directives import CustomGalleryItemDirective

# These lines added to enable Sphinx to work without installing Ray.
import mock


class ChildClassMock(mock.MagicMock):
    @classmethod
    def __getattr__(cls, name):
        return mock.Mock


MOCK_MODULES = [
    "ax",
    "ax.service.ax_client",
    "blist",
    "gym",
    "gym.spaces",
    "horovod",
    "horovod.ray",
    "kubernetes",
    "psutil",
    "ray._raylet",
    "ray.core.generated",
    "ray.core.generated.common_pb2",
    "ray.core.generated.gcs_pb2",
    "ray.core.generated.ray.protocol.Task",
    "scipy.signal",
    "scipy.stats",
    "setproctitle",
    "tensorflow_probability",
    "tensorflow",
    "tensorflow.contrib",
    "tensorflow.contrib.all_reduce",
    "tree",
    "tensorflow.contrib.all_reduce.python",
    "tensorflow.contrib.layers",
    "tensorflow.contrib.rnn",
    "tensorflow.contrib.slim",
    "tensorflow.core",
    "tensorflow.core.util",
    "tensorflow.keras",
    "tensorflow.python",
    "tensorflow.python.client",
    "tensorflow.python.util",
    "torch",
    "torch.distributed",
    "torch.nn",
    "torch.nn.parallel",
    "torch.utils.data",
    "torch.utils.data.distributed",
    "wandb",
    "xgboost",
    "zoopt",
]
import scipy.stats
import scipy.linalg

for mod_name in MOCK_MODULES:
    sys.modules[mod_name] = mock.Mock()
# ray.rllib.models.action_dist.py and
# ray.rllib.models.lstm.py will use tf.VERSION
sys.modules["tensorflow"].VERSION = "9.9.9"
sys.modules["tensorflow.keras.callbacks"] = ChildClassMock()
sys.modules["pytorch_lightning"] = ChildClassMock()

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath("../../python/"))

import ray

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_click.ext',
    'sphinx_tabs.tabs',
    'sphinx-jsonschema',
    'sphinx_gallery.gen_gallery',
    'sphinxemoji.sphinxemoji',
    'sphinx_copybutton',
    'versionwarning.extension',
]

versionwarning_messages = {
    "master": (
        "This document is for the master branch. "
        'Visit the <a href="/en/latest/">latest pip release documentation here</a>.'
    ),
    "latest": (
        "This document is for the latest pip release. "
        'Visit the <a href="/en/master/">master branch documentation here</a>.'
    ),
}

versionwarning_body_selector = "div.document"
sphinx_gallery_conf = {
    "examples_dirs": ["../examples",
                      "tune/_tutorials"],  # path to example scripts
    # path where to save generated examples
    "gallery_dirs": ["auto_examples", "tune/tutorials"],
    "ignore_pattern": "../examples/doc_code/",
    "plot_gallery": "False",
    # "filename_pattern": "tutorial.py",
    # "backreferences_dir": "False",
    # "show_memory': False,
    # 'min_reported_time': False
}

for i in range(len(sphinx_gallery_conf["examples_dirs"])):
    gallery_dir = sphinx_gallery_conf["gallery_dirs"][i]
    source_dir = sphinx_gallery_conf["examples_dirs"][i]
    try:
        os.mkdir(gallery_dir)
    except OSError:
        pass

    # Copy rst files from source dir to gallery dir.
    for f in glob.glob(os.path.join(source_dir, '*.rst')):
        shutil.copy(f, gallery_dir)

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
from recommonmark.parser import CommonMarkParser

# The suffix of source filenames.
source_suffix = ['.rst', '.md']

source_parsers = {
    '.md': CommonMarkParser,
}

# The encoding of source files.
#source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Ray'
copyright = u'2019, The Ray Team'
author = u'The Ray Team'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
from ray import __version__ as version
# The full version, including alpha/beta/rc tags.
release = version

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
#today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build']
exclude_patterns += sphinx_gallery_conf['examples_dirs']

# The reST default role (used for this markup: `text`) to use for all
# documents.
#default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
#add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
#modindex_common_prefix = []

# If true, keep warnings as "system message" paragraphs in the built documents.
#keep_warnings = False

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
import sphinx_rtd_theme
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#html_theme_options = {}

# Add any paths that contain custom themes here, relative to this directory.
#html_theme_path = []

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
#html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
#html_logo = None

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
#html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#html_extra_path = []

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
#html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
html_sidebars = {'**': ['index.html']}

# Additional templates that should be rendered to pages, maps page names to
# template names.
#html_additional_pages = {}

# If false, no module index is generated.
#html_domain_indices = True

# If false, no index is generated.
#html_use_index = True

# If true, the index is split into individual pages for each letter.
#html_split_index = False

# If true, links to the reST sources are added to the pages.
#html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
#html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
#html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
#html_file_suffix = None

# Language to be used for generating the HTML full-text search index.
# Sphinx supports the following languages:
#   'da', 'de', 'en', 'es', 'fi', 'fr', 'hu', 'it', 'ja'
#   'nl', 'no', 'pt', 'ro', 'ru', 'sv', 'tr'
#html_search_language = 'en'

# A dictionary with options for the search language support, empty by default.
# Now only 'ja' uses this config value
#html_search_options = {'type': 'default'}

# The name of a javascript file (relative to the configuration directory) that
# implements a search results scorer. If empty, the default will be used.
#html_search_scorer = 'scorer.js'

# Output file base name for HTML help builder.
htmlhelp_basename = 'Raydoc'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #'papersize': 'letterpaper',

    # The font size ('10pt', '11pt' or '12pt').
    #'pointsize': '10pt',

    # Additional stuff for the LaTeX preamble.
    #'preamble': '',

    # Latex figure (float) alignment
    #'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, 'Ray.tex', u'Ray Documentation', u'The Ray Team', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#latex_use_parts = False

# If true, show page references after internal links.
#latex_show_pagerefs = False

# If true, show URL addresses after external links.
#latex_show_urls = False

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
#latex_domain_indices = True

# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [(master_doc, 'ray', u'Ray Documentation', [author], 1)]

# If true, show URL addresses after external links.
#man_show_urls = False

# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'Ray', u'Ray Documentation', author, 'Ray',
     'One line description of project.', 'Miscellaneous'),
]

# Documents to append as an appendix to all manuals.
#texinfo_appendices = []

# If false, no module index is generated.
#texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
#texinfo_show_urls = 'footnote'

# If true, do not generate a @detailmenu in the "Top" node's menu.
#texinfo_no_detailmenu = False

# pcmoritz: To make the following work, you have to run
# sudo pip install recommonmark

# Python methods should be presented in source code order
autodoc_member_order = 'bysource'

# Taken from https://github.com/edx/edx-documentation
FEEDBACK_FORM_FMT = "https://github.com/ray-project/ray/issues/new?title={title}&labels=docs&body={body}"


def feedback_form_url(project, page):
    """Create a URL for feedback on a particular page in a project."""
    return FEEDBACK_FORM_FMT.format(
        title=urllib.parse.quote(
            "[docs] Issue on `{page}.rst`".format(page=page)),
        body=urllib.parse.quote(
            "# Documentation Problem/Question/Comment\n"
            "<!-- Describe your issue/question/comment below. -->\n"
            "<!-- If there are typos or errors in the docs, feel free to create a pull-request. -->\n"
            "\n\n\n\n"
            "(Created directly from the docs)\n"))


def update_context(app, pagename, templatename, context, doctree):
    """Update the page rendering context to include ``feedback_form_url``."""
    context['feedback_form_url'] = feedback_form_url(app.config.project,
                                                     pagename)


# see also http://searchvoidstar.tumblr.com/post/125486358368/making-pdfs-from-markdown-on-readthedocsorg-using


def setup(app):
    app.connect('html-page-context', update_context)
    app.add_stylesheet('css/custom.css')
    # Custom directives
    app.add_directive('customgalleryitem', CustomGalleryItemDirective)
