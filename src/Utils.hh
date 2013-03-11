//------------------------------------------------------------------------------
// Copyright (c) 2012-2013 by European Organization for Nuclear Research (CERN)
// Author: Justin Salmon <jsalmon@cern.ch>
//------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

#ifndef UTILS_HH_
#define UTILS_HH_

#include "PyXRootD.hh"

#include "XrdCl/XrdClXRootDResponses.hh"

namespace PyXRootD
{
  //----------------------------------------------------------------------------
  //! Convert a C++ type to its corresponding Python binding type. We cast
  //! the object to a void * before packing it into a PyCObject.
  //!
  //! Note: The PyCObject API is deprecated as of Python 2.7
  //----------------------------------------------------------------------------
  template<class Type>
  PyObject* ConvertType( Type *type, PyTypeObject *bindType );

  //----------------------------------------------------------------------------
  //! Convert an XRootDStatus object to a Python dictionary
  //----------------------------------------------------------------------------
  PyObject* XRootDStatusDict( XrdCl::XRootDStatus *status );

  //----------------------------------------------------------------------------
  //! Check that the given callback is actually callable.
  //----------------------------------------------------------------------------
  bool IsCallable( PyObject *callable );

  //----------------------------------------------------------------------------
  //! Initialize the Python types for the extension.
  //----------------------------------------------------------------------------
  int InitTypes();
}

#endif /* UTILS_HH_ */
