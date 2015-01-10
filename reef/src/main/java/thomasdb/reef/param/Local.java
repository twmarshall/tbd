/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package thomasdb.reef.param;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Command line parameter = true to run locally, or false to run on YARN.
 */
@NamedParameter(
    doc = "Whether or not to run on the local runtime",
    short_name = "local",
    default_value = "true")
public final class Local implements Name<Boolean> {

}
