import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordSourceWritableKey implements WritableComparable<WordSourceWritableKey> {
    private String word;
    private String source;

    public WordSourceWritableKey() { }

    public WordSourceWritableKey(String word, String source) {
        this.set(word, source);
    }

    public void set(String word, String source) {
        this.word = word;
        this.source = source;
    }

    public String getWord() {
        return this.word;
    }

    public String getSource() {
        return this.source;
    }

    @Override
    public int compareTo(WordSourceWritableKey o) {
        int cmp = word.compareTo(o.word);
        if( 0 != cmp){
            return cmp;
        }
        return source.compareTo(o.source);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeUTF(source);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readUTF();
        source = dataInput.readUTF();
    }
}
