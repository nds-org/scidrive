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
package edu.jhu.pha.vospace.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import edu.jhu.pha.vospace.api.exceptions.InternalServerErrorException;

public class RegionsInfo {
	private HashSet<RegionDescription> regions = new HashSet<RegionDescription>();
	private static final ObjectMapper mapper = new ObjectMapper();
	
	public HashSet<RegionDescription> getRegions() {
		return regions;
	}

	public void setRegions(HashSet<RegionDescription> regions) {
		this.regions = regions;
	}

	public byte[] toJson() {
		try {
			return mapper.writeValueAsBytes(this.regions);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e, "Error serializing account info to JSON");
		} catch (JsonMappingException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e, "Error serializing account info to JSON");
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e, "Error serializing account info to JSON");
		}
	}
	
	public static class RegionDescription {
		private String id;
		private String url;
		private String display;
		private boolean isDefaultRegion;
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public String getDisplay() {
			return display;
		}
		public void setDisplay(String display) {
			this.display = display;
		}
		public boolean isDefaultRegion() {
			return isDefaultRegion;
		}
		public void setDefault(boolean isDefaultRegion) {
			this.isDefaultRegion = isDefaultRegion;
		}
	}
}
