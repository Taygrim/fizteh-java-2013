package ru.fizteh.fivt.students.demidov.storeable;

import java.io.IOException;
import ru.fizteh.fivt.storage.structured.TableProviderFactory;

public class StoreableTableProviderFactory implements TableProviderFactory {
	public StoreableTableProviderFactory() {}
	
	public StoreableTableProvider create(String dir) throws IOException {
		if ((dir == null) || (dir.trim().isEmpty())) {
			throw new IllegalArgumentException("wrong dir");
		}
		return new StoreableTableProvider(dir);			
	}
}