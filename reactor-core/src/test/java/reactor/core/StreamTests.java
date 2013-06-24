/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import reactor.AbstractReactorTest;
import reactor.fn.Consumer;

/**
 * @author Sergey Shcherbakov
 */
public class StreamTests extends AbstractReactorTest {
	
	@Test
	public void testContinuousBatching() throws InterruptedException {
		final int TOTAL_ITEMS = 8;
		final int BATCH_SIZE = 3;
		final CountDownLatch latch = new CountDownLatch(TOTAL_ITEMS/BATCH_SIZE+1);
		final List<String> results = new ArrayList<String>();
		
		Stream<String> head = Streams.<String>defer().get();
		head.setExpectedAcceptCount(BATCH_SIZE).reduce().consume(new Consumer<List<String>>() {
			@Override
			public void accept(List<String> li) {
				latch.countDown();
				results.addAll(li);
			}
		});
		
		for(int i=0; i<TOTAL_ITEMS; i++) {
			head.accept("Obj"+i);
		}
		head.complete();
	
		latch.await(1, TimeUnit.SECONDS);
		assertThat(latch.getCount(), is(0L));
		assertThat(results.size(), equalTo(TOTAL_ITEMS));
	}
}
