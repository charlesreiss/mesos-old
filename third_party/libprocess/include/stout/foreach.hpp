/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __STOUT_FOREACH_HPP__
#define __STOUT_FOREACH_HPP__

#undef foreach

#include <google/protobuf/repeated_field.h>

#include <boost/foreach.hpp>

#include <boost/tuple/tuple.hpp>

namespace boost {
namespace foreach {

template <class T>
struct is_noncopyable< google::protobuf::RepeatedField<T> > : mpl::true_ {};

template <class T>
struct is_noncopyable< google::protobuf::RepeatedPtrField<T> > : mpl::true_ {};

}  // namespace foreach
}  // namespace boost


#define BOOST_FOREACH_PAIR(VARFIRST, VARSECOND, COL)                                            \
    BOOST_FOREACH_PREAMBLE()                                                                    \
    if (boost::foreach_detail_::auto_any_t BOOST_FOREACH_ID(_foreach_col) = BOOST_FOREACH_CONTAIN(COL)) {} else   \
    if (boost::foreach_detail_::auto_any_t BOOST_FOREACH_ID(_foreach_cur) = BOOST_FOREACH_BEGIN(COL)) {} else     \
    if (boost::foreach_detail_::auto_any_t BOOST_FOREACH_ID(_foreach_end) = BOOST_FOREACH_END(COL)) {} else       \
    for (bool BOOST_FOREACH_ID(_foreach_continue) = true, BOOST_FOREACH_ID(_foreach_onetime) = true;              \
              BOOST_FOREACH_ID(_foreach_continue) && !BOOST_FOREACH_DONE(COL);                                    \
              BOOST_FOREACH_ID(_foreach_continue) ? BOOST_FOREACH_NEXT(COL) : (void)0)                            \
        if  (boost::foreach_detail_::set_false(BOOST_FOREACH_ID(_foreach_onetime))) {} else                       \
        for (VARFIRST = BOOST_FOREACH_DEREF(COL).first;                                         \
	     !BOOST_FOREACH_ID(_foreach_onetime);                                               \
	     BOOST_FOREACH_ID(_foreach_onetime) = true)                                         \
            if  (boost::foreach_detail_::set_false(BOOST_FOREACH_ID(_foreach_continue))) {} else \
            for (VARSECOND = BOOST_FOREACH_DEREF(COL).second;                                   \
		 !BOOST_FOREACH_ID(_foreach_continue);                                          \
		 BOOST_FOREACH_ID(_foreach_continue) = true)

#undef foreach
#define foreach(VAR, LIST) BOOST_FOREACH(VAR, LIST)
#undef foreachpair
#define foreachpair(VAR1, VAR2, LIST) BOOST_FOREACH_PAIR(VAR1, VAR2, LIST)

namespace mesos {
namespace internal {
namespace foreach_util {

struct IgnoreAssignment {
  IgnoreAssignment() {}
  template <class T>
  void operator=(const T&) const {}
};

static const IgnoreAssignment ignore;

}  // namespace foreach_util
}  // namespace internal
}  // namespace mesos

#define foreachkey(VAR, COL)                    \
  foreachpair (VAR, mesos::internal::foreach_util::ignore, COL)

#define foreachvalue(VAR, COL)                  \
  foreachpair (mesos::internal::foreach_util::ignore, VAR, COL)

#endif // __STOUT_FOREACH_HPP__
