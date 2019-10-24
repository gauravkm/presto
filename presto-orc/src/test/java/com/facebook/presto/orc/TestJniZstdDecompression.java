/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import com.facebook.presto.orc.zstd.ZstdJniCompressor;
import com.facebook.presto.orc.zstd.ZstdJniDecompressor;
import com.facebook.presto.testing.assertions.Assert;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Random;

public class TestJniZstdDecompression
{
    private static final DataSize MAX_BUFFER_SIZE = new DataSize(4, DataSize.Unit.MEGABYTE);
    private final ZstdJniCompressor compressor = new ZstdJniCompressor();
    private final ZstdJniDecompressor decompressor = new ZstdJniDecompressor(new OrcDataSourceId("test"), MAX_BUFFER_SIZE.toBytes());

    @Test
    public void testDecompression()
            throws OrcCorruptionException
    {
        byte[] sourceBytes = generateRandomBytes();
        byte[] compressedBytes = new byte[1024 * 1024];
        int size = compressor.compress(sourceBytes, 0, sourceBytes.length, compressedBytes, 0, compressedBytes.length);
        int outputSize = decompressor.decompress(compressedBytes, 0, size,
                new OrcDecompressor.OutputBuffer()
                {
                    @Override
                    public byte[] initialize(int size)
                    {
                        return new byte[size];
                    }

                    @Override
                    public byte[] grow(int size)
                    {
                        throw new RuntimeException();
                    }
                });
        Assert.assertEquals(outputSize, sourceBytes.length);
    }

    private byte[] generateRandomBytes()
    {
        Random random = new Random();
        byte[] array = new byte[1024];
        random.nextBytes(array);
        return array;
    }
}
