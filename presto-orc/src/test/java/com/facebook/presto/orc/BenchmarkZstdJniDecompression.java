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

import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.zstd.ZstdJniCompressor;
import com.facebook.presto.orc.zstd.ZstdJniDecompressor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)

public class BenchmarkZstdJniDecompression
{
    private static final ZstdJniCompressor compressor = new ZstdJniCompressor();
    private static final List<Unit> list = generateWorkload();
    private static final int sourceLength = 256 * 1024;
    private static byte[] decompressedBytes = new byte[sourceLength];
    private final ZstdJniDecompressor jniDecompressor = new ZstdJniDecompressor(new OrcDataSourceId("sample"), sourceLength);
    private final OrcZstdDecompressor orcJavaDecompressor = new OrcZstdDecompressor(new OrcDataSourceId("orc"), sourceLength);
    private static final DataSize SIZE = new DataSize(1, MEGABYTE);
    private static byte[] data;

    static {
        try {
            data = loadData();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static byte[] loadData()
            throws IOException
    {
        return toByteArray(getResource("sample_orc_zstd"));
    }

    private OrcReader createReader(boolean zstdJniDecompressionEnabled)
            throws IOException
    {
        return new OrcReader(
                new InMemoryOrcDataSource(data),
                DWRF,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                new OrcReaderOptions(
                        SIZE,
                        SIZE,
                        SIZE,
                        zstdJniDecompressionEnabled));
    }

    @Benchmark
    public void benchmark_decompress_file_jni()
            throws IOException
    {
        decompressJni_file(true);
    }

    @Benchmark
    public void benchmark_decompress_file_java()
            throws IOException
    {
        decompressJni_file(false);
    }

    @Test
    public void decompressJni_file(boolean zstdJniDecompressionEnabled)
            throws IOException
    {
        OrcReader reader = createReader(zstdJniDecompressionEnabled);

        assertEquals(reader.getCompressionKind(), CompressionKind.ZSTD);

        int columnIndexDouble = 0;
        int columnIndexString = 6;
        int columnIndexLong = 5;
        int columnIndexList = 100;
        CharType charType = createCharType(10);
        VarcharType varchar = VarcharType.VARCHAR;
        ArrayType arrayType = new ArrayType(varchar);
        Map<Integer, Type> includedColumns = ImmutableMap.<Integer, Type>builder()
                .put(columnIndexDouble, DOUBLE)
                .put(columnIndexLong, BIGINT)
                .put(columnIndexString, charType)
                .put(columnIndexList, arrayType)
                .build();

        OrcBatchRecordReader batchRecordReader = reader.createBatchRecordReader(
                includedColumns,
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE);

        int rows = 0;
        int size = 0;
        while (true) {
            int batchSize = batchRecordReader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            rows += batchSize;

            Block xBlock = batchRecordReader.readBlock(DOUBLE, columnIndexDouble);
            Block yBlock = batchRecordReader.readBlock(BIGINT, columnIndexLong);
            Block zBlock = batchRecordReader.readBlock(charType, columnIndexString);
            Block aBlock = batchRecordReader.readBlock(arrayType, columnIndexList);

            size += xBlock.getPositionCount();
            size += yBlock.getPositionCount();
            size += zBlock.getPositionCount();
            for (int position = 0; position < batchSize; position++) {
                DOUBLE.getDouble(xBlock, position);
                BIGINT.getLong(yBlock, position);
                charType.getSlice(zBlock, position);
                arrayType.getObject(aBlock, position);
            }
        }

        assertEquals(rows, reader.getFooter().getNumberOfRows());
        assertEquals(size, 263598);
    }

    @Benchmark
    public void decompressJni()
            throws OrcCorruptionException
    {
        decompressList(jniDecompressor);
    }

    @Benchmark
    public void decompressJava()
            throws OrcCorruptionException
    {
        decompressList(orcJavaDecompressor);
    }

    private void decompressList(OrcDecompressor decompressor)
            throws OrcCorruptionException
    {
        for (Unit unit : list) {
            int outputSize = decompressor.decompress(unit.compressedBytes, 0, unit.compressedLength, new OrcDecompressor.OutputBuffer()
            {
                @Override
                public byte[] initialize(int size)
                {
                    return decompressedBytes;
                }

                @Override
                public byte[] grow(int size)
                {
                    throw new RuntimeException();
                }
            });
            assertEquals(outputSize, unit.sourceLength);
        }
    }

    private static List<Unit> generateWorkload()
    {
        ImmutableList.Builder<Unit> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < 10; i++) {
            byte[] srcBytes = getAlphaNumericString(sourceLength).getBytes();
            byte[] compressedBytes = new byte[sourceLength * 32];
            int size = compressor.compress(srcBytes, 0, srcBytes.length, compressedBytes, 0, compressedBytes.length);
            builder.add(new Unit(srcBytes, sourceLength, compressedBytes, size));
        }
        return builder.build();
    }

    static String getAlphaNumericString(int length)
    {
        String alphaNumericString = "USINDIA";

        StringBuilder stringBuilder = new StringBuilder(length);

        for (int index = 0; index < length; index++) {
            int arrayIndex = (int) (alphaNumericString.length() * Math.random());

            stringBuilder.append(alphaNumericString.charAt(arrayIndex));
        }
        return stringBuilder.toString();
    }

    static class Unit
    {
        byte[] sourceBytes;
        int sourceLength;
        byte[] compressedBytes;
        int compressedLength;

        public Unit(byte[] sourceBytes, int sourceLength, byte[] compressedBytes, int compressedLength)
        {
            this.sourceBytes = sourceBytes;
            this.sourceLength = sourceLength;
            this.compressedBytes = compressedBytes;
            this.compressedLength = compressedLength;
        }
    }

    private static class InMemoryOrcDataSource
            extends AbstractOrcDataSource
    {
        private final byte[] data;

        public InMemoryOrcDataSource(byte[] data)
        {
            super(new OrcDataSourceId("memory"), data.length, SIZE, SIZE, SIZE, false);
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            System.arraycopy(data, toIntExact(position), buffer, bufferOffset, bufferLength);
        }
    }
}
