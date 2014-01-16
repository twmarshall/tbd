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
package tbd.messages

import tbd.ddg.ReadId
import tbd.mod.Mod
import tbd.mod.ModId

// DDG
case class AddReadMessage(modId: ModId, readId: ReadId)
case class AddWriteMessage(readId: ReadId, modId: ModId)
case class AddCallMessage(outerCall: ReadId, innerCall: ReadId)
case class ToStringMessage

// Worker
case class FinishedMessage(result: Mod[Any])

// Input
case class GetMessage(key: Int)
case class PutMessage(key: Int, value: String)
case class GetSizeMessage
case class GetArrayMessage
case class GetListMessage

// ModStore
case class ReadModMessage(modId: ModId)
case class WriteModMessage(value: Any)
case class WriteNullModMessage
case class NullValueMessage
