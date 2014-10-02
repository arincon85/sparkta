/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
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
package com.stratio.sparkta.driver.factory

import com.stratio.sparkta.driver.exception.DriverException

/**
 * Created by ajnavarro on 2/10/14.
 */
object PropertyValidationHandler {
  def validateProperty(property: String, configuration: Map[String, String],
                       errorMessage: String = "%s property is mandatory."): String = {
    assert(configuration.contains(property), throw new DriverException(errorMessage.format(property)))
    configuration.get(property).get
  }
}