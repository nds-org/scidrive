/*******************************************************************************
 * Copyright 2013 Johns Hopkins University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package edu.jhu.pha.vospace.process;

import java.util.ArrayList;

import org.apache.tika.metadata.Metadata;
import org.codehaus.jackson.JsonNode;

import edu.jhu.pha.vospace.process.database.Database;
import edu.jhu.pha.vospace.process.database.SQLShare;
import edu.jhu.pha.vospace.process.sax.AsciiTable;
import edu.jhu.pha.vospace.process.sax.AsciiTableContentHandler;

public class FileToSQLShareProcessor {

	public static void processNodeMeta(Metadata metadata, Object handler, JsonNode credentials) throws Exception {
		Database db = new SQLShare(credentials);
		db.setup();
		db.update(metadata, (ArrayList<AsciiTable>)((AsciiTableContentHandler)handler).getTables());
  	}
}
